import numpy as np
import sys
import json
import os
import time
import struct
import warnings
from collections import OrderedDict
from io import BytesIO
from ndstorage.file_io import NDTiffFileIO, BUILTIN_FILE_IO
from ndstorage.ndtiff_index import NDTiffIndexEntry

from collections import deque
from concurrent.futures import ThreadPoolExecutor

MAJOR_VERSION = 3
MINOR_VERSION = 3

# Constants for writing files
BYTES_PER_GIG = 1073741824
MAX_FILE_SIZE = 4 * BYTES_PER_GIG

ENTRIES_PER_IFD = 13
# Required tags
WIDTH = 256
HEIGHT = 257
BITS_PER_SAMPLE = 258
COMPRESSION = 259
PHOTOMETRIC_INTERPRETATION = 262
IMAGE_DESCRIPTION = 270
STRIP_OFFSETS = 273
SAMPLES_PER_PIXEL = 277
ROWS_PER_STRIP = 278
STRIP_BYTE_COUNTS = 279
X_RESOLUTION = 282
Y_RESOLUTION = 283
RESOLUTION_UNIT = 296
MM_METADATA = 51123

SUMMARY_MD_HEADER = 2355492


_POSITION_AXIS = "position"
_ROW_AXIS = "row"
_COLUMN_AXIS = "column"
_Z_AXIS = "z"
_TIME_AXIS = "time"
_CHANNEL_AXIS = "channel"

class SingleNDTiffWriter:

    def __init__(self, directory, filename, summary_md):
        self.filename = os.path.join(directory, filename)
        self.index_map = {}
        self.next_ifd_offset_location = -1
        self.res_numerator = 1
        self.res_denominator = 1
        self.z_step_um = 1
        self.buffers = deque()
        self.first_ifd = True

        self.start_time = None

        os.makedirs(directory, exist_ok=True)
        # pre-allocate the file
        file_path = os.path.join(directory, filename)
        with open(file_path, 'wb') as f:
            f.seek(MAX_FILE_SIZE - 1)
            f.write(b'\0')
            f.flush()

        # reopen the file in binary mode
        self.file = open(file_path, 'rb+')
        # reset position to 0
        self.file.seek(0)

        self._write_mm_header_and_summary_md(summary_md)
        self.reader = SingleNDTiffReader(self.filename, summary_md=summary_md)

    def has_space_to_write(self, pixels, metadata):
        rgb = pixels.ndim == 3 and pixels.shape[2] == 3
        md_length = len(metadata)
        IFD_size = ENTRIES_PER_IFD * 12 + 4 + 16
        extra_padding = 5000000  # 5 MB extra padding
        bytes_per_pixels = self._bytes_per_image_pixels(pixels, rgb)

        size = md_length + IFD_size + bytes_per_pixels + extra_padding + self.file.tell()

        if size >= MAX_FILE_SIZE:
            return False
        return True

    def _write_mm_header_and_summary_md(self, summary_md):
        summary_md_bytes = self._get_bytes_from_string(json.dumps(summary_md))
        md_length = len(summary_md_bytes)
        header_buffer = bytearray(28)

        # 8 bytes for file header
        if sys.byteorder == 'big':
            struct.pack_into('>H', header_buffer, 0, 0x4D4D)
        else:
            struct.pack_into('<H', header_buffer, 0, 0x4949)
        struct.pack_into('<H', header_buffer, 2, 42)
        first_ifd_offset = 28 + md_length
        if first_ifd_offset % 2 == 1:
            first_ifd_offset += 1  # Start first IFD on a word
        struct.pack_into('<I', header_buffer, 4, first_ifd_offset)

        # 12 bytes for unique identifier and major version
        struct.pack_into('<III', header_buffer, 8, 483729, MAJOR_VERSION, MINOR_VERSION)

        # 8 bytes for summaryMD header and summary md length
        struct.pack_into('<II', header_buffer, 20, SUMMARY_MD_HEADER, md_length)

        for buffer in [header_buffer, summary_md_bytes]:
            self.file.write(buffer)

    def _get_bytes_from_string(self, s):
        return s.encode('utf-8')

    def finished_writing(self):
        self._write_null_offset_after_last_image()
        self.file.truncate()
        self.file.flush()
        self.file.close()

    def _write_null_offset_after_last_image(self):
        buffer = bytearray(4)
        struct.pack_into('<I', buffer, 0, 0)
        current_pos = self.file.tell()
        self.file.seek(self.next_ifd_offset_location)
        self.file.write(buffer)
        self.file.seek(current_pos)

    def write_image(self, index_key, pixels, metadata, bit_depth='auto'):
        """
        Write an image to the file

        Parameters
        ----------
        index_key : frozenset
            The key to index the image
        pixels : np.ndarray or bytearray
            The image data
        metadata : dict or str
            The metadata for the image
        bit_depth : int
            The bit depth of the image

        Returns
        -------
        NDTiffIndexEntry
            The index entry for the image
        """
        image_height, image_width = pixels.shape
        rgb = pixels.ndim == 3 and pixels.shape[2] == 3
        if bit_depth == 'auto':
            bit_depth = 8 if pixels.dtype == np.uint8 else 16
        # if metadata is a dict, serialize it to a json string and make it a utf8 byte buffer
        if isinstance(metadata, dict):
            metadata = self._get_bytes_from_string(json.dumps(metadata))
        ied = self._write_ifd(index_key, pixels, metadata, rgb, image_height, image_width, bit_depth)
        while self.buffers:
            self.file.write(self.buffers.popleft())
        self.index_map[index_key] = ied
        return ied


    def _write_ifd(self, index_key, pixels, metadata, rgb, image_height, image_width, bit_depth):
        if self.file.tell() % 2 == 1:
            self.file.seek(self.file.tell() + 1)  # Make IFD start on word

        byte_depth = 1 if isinstance(pixels, bytearray) else 2
        bytes_per_image_pixels = self._bytes_per_image_pixels(pixels, rgb)
        num_entries = 13

        # 2 bytes for number of directory entries, 12 bytes per directory entry, 4 byte offset of next IFD
        # 6 bytes for bits per sample if RGB, 16 bytes for x and y resolution, 1 byte per character of MD string
        # number of bytes for pixels
        ifd_and_bit_depth_bytes = 2 + num_entries * 12 + 4 + (6 if rgb else 0) + 16
        ifd_and_small_vals_buffer = bytearray(ifd_and_bit_depth_bytes)

        # Needed to reset to zero after last IFD
        self.next_ifd_offset_location = self.file.tell() + 2 + num_entries * 12
        bits_per_sample_offset = self.next_ifd_offset_location + 4
        x_resolution_offset = bits_per_sample_offset + (6 if rgb else 0)
        y_resolution_offset = x_resolution_offset + 8
        pixel_data_offset = y_resolution_offset + 8
        metadata_offset = pixel_data_offset + bytes_per_image_pixels

        next_ifd_offset = metadata_offset + len(metadata)
        if next_ifd_offset % 2 == 1:
            next_ifd_offset += 1  # Make IFD start on word

        buffer_position = 0
        struct.pack_into('<H', ifd_and_small_vals_buffer, buffer_position, num_entries)
        buffer_position += 2

        buffer_position += self._write_ifd_entry(ifd_and_small_vals_buffer, WIDTH, 4, 1, image_width, buffer_position)
        buffer_position += self._write_ifd_entry(ifd_and_small_vals_buffer, HEIGHT, 4, 1, image_height, buffer_position)
        buffer_position += self._write_ifd_entry(ifd_and_small_vals_buffer, BITS_PER_SAMPLE, 3, 3 if rgb else 1,
                                                 bits_per_sample_offset if rgb else byte_depth * 8, buffer_position)
        buffer_position += self._write_ifd_entry(ifd_and_small_vals_buffer, COMPRESSION, 3, 1, 1, buffer_position)
        buffer_position += self._write_ifd_entry(ifd_and_small_vals_buffer, PHOTOMETRIC_INTERPRETATION, 3, 1,
                                                 2 if rgb else 1, buffer_position)
        buffer_position += self._write_ifd_entry(ifd_and_small_vals_buffer, STRIP_OFFSETS, 4, 1, pixel_data_offset,
                                                 buffer_position)
        buffer_position += self._write_ifd_entry(ifd_and_small_vals_buffer, SAMPLES_PER_PIXEL, 3, 1, 3 if rgb else 1,
                                                 buffer_position)
        buffer_position += self._write_ifd_entry(ifd_and_small_vals_buffer, ROWS_PER_STRIP, 3, 1, image_height,
                                                 buffer_position)
        buffer_position += self._write_ifd_entry(ifd_and_small_vals_buffer, STRIP_BYTE_COUNTS, 4, 1,
                                                 bytes_per_image_pixels, buffer_position)
        buffer_position += self._write_ifd_entry(ifd_and_small_vals_buffer, X_RESOLUTION, 5, 1, x_resolution_offset,
                                                 buffer_position)
        buffer_position += self._write_ifd_entry(ifd_and_small_vals_buffer, Y_RESOLUTION, 5, 1, y_resolution_offset,
                                                 buffer_position)
        buffer_position += self._write_ifd_entry(ifd_and_small_vals_buffer, RESOLUTION_UNIT, 3, 1, 3, buffer_position)
        buffer_position += self._write_ifd_entry(ifd_and_small_vals_buffer, MM_METADATA, 2, len(metadata),
                                                 metadata_offset, buffer_position)

        struct.pack_into('<I', ifd_and_small_vals_buffer, buffer_position, next_ifd_offset)
        buffer_position += 4

        if rgb:
            struct.pack_into('<HHH', ifd_and_small_vals_buffer, buffer_position, byte_depth * 8, byte_depth * 8,
                             byte_depth * 8)
            buffer_position += 6

        struct.pack_into('<II', ifd_and_small_vals_buffer, buffer_position, self.res_numerator, self.res_denominator)
        buffer_position += 8
        struct.pack_into('<II', ifd_and_small_vals_buffer, buffer_position, self.res_numerator, self.res_denominator)
        buffer_position += 8

        self.buffers.append(ifd_and_small_vals_buffer)
        self.buffers.append(self._get_pixel_buffer(pixels, rgb))
        self.buffers.append(metadata)

        self.first_ifd = False

        # Return structured data for putting into the index entry
        pixel_type = {
            8: NDTiffIndexEntry.EIGHT_BIT,
            10: NDTiffIndexEntry.TEN_BIT,
            12: NDTiffIndexEntry.TWELVE_BIT,
            14: NDTiffIndexEntry.FOURTEEN_BIT,
            16: NDTiffIndexEntry.SIXTEEN_BIT,
            11: NDTiffIndexEntry.ELEVEN_BIT
        }.get(bit_depth, NDTiffIndexEntry.EIGHT_BIT_RGB if rgb else None)

        return NDTiffIndexEntry(index_key, pixel_type, pixel_data_offset, image_width, image_height, metadata_offset,
                                len(metadata), self.filename.split(os.sep)[-1])

    def _write_ifd_entry(self, buffer, tag, dtype, count, value, buffer_position):
        struct.pack_into('<HHII', buffer, buffer_position, tag, dtype, count, value)
        return 12

    def _get_pixel_buffer(self, pixels, rgb):
        if rgb:
            original_pix = pixels
            rgb_pix = bytearray(len(original_pix) * 3 // 4)
            num_pix = len(original_pix) // 4
            for i in range(num_pix):
                rgb_pix[i * 3] = original_pix[i * 4 + 2]
                rgb_pix[i * 3 + 1] = original_pix[i * 4 + 1]
                rgb_pix[i * 3 + 2] = original_pix[i * 4]
            return rgb_pix
        else:
            return pixels

    def _bytes_per_image_pixels(self, pixels, rgb):
        if rgb:
            return len(pixels) * 3 // 4
        else:
            if isinstance(pixels, bytearray):
                return len(pixels)
            elif isinstance(pixels, np.ndarray) and pixels.dtype == np.uint16:
                return pixels.size * 2
            else:
                raise RuntimeError("unknown pixel type")


class SingleNDTiffReader:
    """
    Class corresponsing to a single multipage tiff file
    Pass the full path of the TIFF to instantiate and call close() when finished
    """

    # file format constants
    SUMMARY_MD_HEADER = 2355492
    EIGHT_BIT_MONOCHROME = 0
    SIXTEEN_BIT_MONOCHROME = 1
    EIGHT_BIT_RGB = 2
    TEN_BIT_MONOCHROME = 3
    TWELVE_BIT_MONOCHROME = 4
    FOURTEEN_BIT_MONOCHROME = 5
    ELEVEN_BIT_MONOCHROME = 6

    UNCOMPRESSED = 0

    def __init__(self, tiff_path, file_io: NDTiffFileIO = BUILTIN_FILE_IO, summary_md=None):
        """
        tiff_path: str
            The path to a .tiff file to load
        file_io: ndtiff.file_io.NDTiffFileIO
            A container containing various methods for interacting with files.
        summary_md: dict
            If not None, this corresponds to a file that is actively being written to by an associated writer
        """
        self.file_io = file_io
        self.tiff_path = tiff_path
        self.file = self.file_io.open(tiff_path, "rb")
        if summary_md is None:
            self.summary_md, self.first_ifd_offset = self._read_header()
        else:
            self.summary_md = summary_md
            self.major_version = MAJOR_VERSION
            self.minor_version = MINOR_VERSION

    def close(self):
        """ """
        self.file.close()

    def _read_header(self):
        """
        Returns
        -------
        summary metadata : dict
        byte offsets : nested dict
            The byte offsets of TIFF Image File Directories with keys [channel_index][z_index][frame_index][position_index]
        first_image_byte_offset : int
            int byte offset of first image IFD
        """
        # read standard tiff header
        if self._read(0, 2) == b"\x4d\x4d":
            # Big endian
            if sys.byteorder != "big":
                raise Exception("Potential issue with mismatched endian-ness")
        elif self._read(0, 2) == b"\x49\x49":
            # little endian
            if sys.byteorder != "little":
                raise Exception("Potential issue with mismatched endian-ness")
        else:
            raise Exception("Endian type not specified correctly")
        if np.frombuffer(self._read(2,4), dtype=np.uint16)[0] != 42:
            raise Exception("Tiff magic 42 missing")
        first_ifd_offset = np.frombuffer(self._read(4,8), dtype=np.uint32)[0]

        # read custom stuff: header, summary md
        self.major_version = int.from_bytes(self._read(12, 16), sys.byteorder)
        self.minor_version = int.from_bytes(self._read(16, 20), sys.byteorder)

        summary_md_header, summary_md_length = np.frombuffer(self._read(20, 28), dtype=np.uint32)
        if summary_md_header != self.SUMMARY_MD_HEADER:
            raise Exception("Summary metadata header wrong")
        summary_md = json.loads(self._read(28, 28 + summary_md_length))
        return summary_md, first_ifd_offset

    def _read(self, start, end):
        """
        convert to python ints
        """
        self.file.seek(int(start), 0)
        return self.file.read(end - start)

    def read_metadata(self, index):
        return json.loads(
            self._read(
                index["metadata_offset"], index["metadata_offset"] + index["metadata_length"]
            )
        )

    def read_image(self, index_entry):
        if index_entry.pixel_type == self.EIGHT_BIT_RGB:
            bytes_per_pixel = 3
            dtype = np.uint8
        elif index_entry.pixel_type == self.EIGHT_BIT_MONOCHROME:
            bytes_per_pixel = 1
            dtype = np.uint8
        elif index_entry.pixel_type == self.SIXTEEN_BIT_MONOCHROME or \
                index_entry.pixel_type == self.TEN_BIT_MONOCHROME or \
                index_entry.pixel_type == self.TWELVE_BIT_MONOCHROME or \
                index_entry.pixel_type == self.FOURTEEN_BIT_MONOCHROME or \
                index_entry.pixel_type == self.ELEVEN_BIT_MONOCHROME:
            bytes_per_pixel = 2
            dtype = np.uint16
        else:
            raise Exception("unrecognized pixel type")
        width = index_entry.image_width
        height = index_entry.image_height
        data = self._read(index_entry.pix_offset, index_entry.pix_offset + width * height * bytes_per_pixel)
        pixels = np.frombuffer(data, dtype=dtype)
        image = pixels.reshape([height, width, 3] if bytes_per_pixel == 3 else [height, width])
        return image
