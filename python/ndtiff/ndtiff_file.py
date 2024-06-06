import numpy as np
import sys
import json
from ndtiff.file_io import NDTiffFileIO, BUILTIN_FILE_IO


_POSITION_AXIS = "position"
_ROW_AXIS = "row"
_COLUMN_AXIS = "column"
_Z_AXIS = "z"
_TIME_AXIS = "time"
_CHANNEL_AXIS = "channel"

_AXIS_ORDER = {_ROW_AXIS: 7,
               _COLUMN_AXIS: 6,
               _POSITION_AXIS: 5, 
               _TIME_AXIS: 4, 
               _CHANNEL_AXIS:3, 
               _Z_AXIS:2}

def _get_axis_order_key(dict_item):
    axis_name = dict_item[0]
    if axis_name in _AXIS_ORDER.keys():
        return _AXIS_ORDER[axis_name]
    else:
        return 3  # stack next to channel axes

class _SingleNDTiffReader:
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

    def __init__(self, tiff_path, file_io: NDTiffFileIO = BUILTIN_FILE_IO):
        """
        tiff_path: str
            The path to a .tiff file to load
        file_io: ndtiff.file_io.NDTiffFileIO
            A container containing various methods for interacting with files.
        """
        self.file_io = file_io
        self.tiff_path = tiff_path
        self.file = self.file_io.open(tiff_path, "rb")
        self.summary_md, self.first_ifd_offset = self._read_header()

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

    def read_image(self, index):
        if index["pixel_type"] == self.EIGHT_BIT_RGB:
            bytes_per_pixel = 3
            dtype = np.uint8
        elif index["pixel_type"] == self.EIGHT_BIT_MONOCHROME:
            bytes_per_pixel = 1
            dtype = np.uint8
        elif index["pixel_type"] == self.SIXTEEN_BIT_MONOCHROME or \
                index["pixel_type"] == self.TEN_BIT_MONOCHROME or \
                index["pixel_type"] == self.TWELVE_BIT_MONOCHROME or \
                index["pixel_type"] == self.FOURTEEN_BIT_MONOCHROME or \
                index["pixel_type"] == self.ELEVEN_BIT_MONOCHROME:
            bytes_per_pixel = 2
            dtype = np.uint16
        else:
            raise Exception("unrecognized pixel type")
        width = index["image_width"]
        height = index["image_height"]
        image = np.reshape(
            np.frombuffer(self._read(
            index["pixel_offset"], index["pixel_offset"] + width * height * bytes_per_pixel)
                , dtype=dtype),
            [height, width, 3] if bytes_per_pixel == 3 else [height, width],
        )
        return image
