import json
import struct
import warnings
from collections import OrderedDict
from io import BytesIO

def read_ndtiff_index(data, verbose=True):
    index = {}
    position = 0
    while position < len(data):
        if verbose:
            print("\rReading index... {:.1f}%       ".format(100 * (1 - (len(data) - position) / len(data))), end="")
        entry = NDTiffIndexEntry.unpack_single_index_entry(data, position)
        if entry is None:
            break
        position, axes, index_entry = entry
        index[frozenset(axes.items())] = index_entry
        if position is None:
            break
    return index


class NDTiffIndexEntry:
    EIGHT_BIT = 0
    SIXTEEN_BIT = 1
    EIGHT_BIT_RGB = 2
    TEN_BIT = 3
    TWELVE_BIT = 4
    FOURTEEN_BIT = 5
    ELEVEN_BIT = 6

    UNCOMPRESSED = 0

    def __init__(self, axes_key, pixel_type, pix_offset, image_width, image_height, md_offset, md_length, filename):
        self.axes_key = axes_key
        self.pix_offset = pix_offset
        self.image_width = image_width
        self.image_height = image_height
        self.metadata_length = md_length
        self.metadata_offset = md_offset
        self.pixel_type = pixel_type
        self.pixel_compression = self.UNCOMPRESSED
        self.metadata_compression = self.UNCOMPRESSED
        self.filename = filename
        self.data_set_finished_entry = axes_key is None

    # for backwards comapatibility when this was a dict
    def __getitem__(self, key):
        return getattr(self, key)

    def __setitem__(self, key, value):
        setattr(self, key, value)

    @classmethod
    def create_finished_entry(cls):
        return cls()

    def __str__(self):
        buffer = self.as_byte_buffer()
        return buffer.getvalue().decode('iso-8859-1')

    def is_data_set_finished_entry(self):
        return self.data_set_finished_entry

    def to_essential_image_metadata(self):
        return EssentialImageMetadata(int(self.pix_width), int(self.pix_height),
                                      self.get_bit_depth(), self.is_rgb())

    @staticmethod
    def unsign_int(i):
        return i & 0xFFFFFFFF

    @staticmethod
    def read_index_map(filepath):
        with open(filepath, 'rb') as f:
            index_map = OrderedDict()
            data = f.read()
            position = 0
            while position < len(data):
                result = NDTiffIndexEntry.unpack_single_index_entry(data, position)
                if result is None:
                    break
                position, axes, index_entry = result
                index_key = NDTiffIndexEntry.serialize_axes(axes)
                index_map[index_key] = NDTiffIndexEntry(
                    index_key, index_entry.pixel_type, index_entry.pixel_offset,
                    index_entry.image_width, index_entry.image_height, index_entry.metadata_offset,
                    index_entry.metadata_length, index_entry.filename
                )
            return index_map

    @staticmethod
    def unpack_single_index_entry(data, position=0):
        """
        Unpacks a single index entry from the data buffer
        """
        (axes_length,) = struct.unpack("I", data[position: position + 4])
        if axes_length == 0:
            warnings.warn(
                "Index appears to not have been properly terminated (the dataset may still work)"
            )
            return None
        axes_str = data[position + 4: position + 4 + axes_length].decode("utf-8")
        axes = json.loads(axes_str)
        position += axes_length + 4
        (filename_length,) = struct.unpack("I", data[position: position + 4])
        filename = data[position + 4: position + 4 + filename_length].decode("utf-8")
        position += 4 + filename_length
        pixel_offset, image_width, image_height, pixel_type, pixel_compression, \
            metadata_offset, metadata_length, metadata_compression = \
            struct.unpack("IIIIIIII", data[position: position + 32])
        index_entry = NDTiffIndexEntry(axes.items, pixel_type, pixel_offset, image_width, image_height,
                                        metadata_offset, metadata_length, filename)
        position += 32
        return position, axes, index_entry


    def is_rgb(self):
        return self.pixel_type == self.EIGHT_BIT_RGB

    def get_byte_depth(self):
        return 2 if self.pixel_type in [self.SIXTEEN_BIT, self.FOURTEEN_BIT, self.TWELVE_BIT, self.TEN_BIT, self.ELEVEN_BIT] else 1

    def get_bit_depth(self):
        bit_depth_map = {
            self.EIGHT_BIT: 8,
            self.TEN_BIT: 10,
            self.TWELVE_BIT: 12,
            self.FOURTEEN_BIT: 14,
            self.SIXTEEN_BIT: 16,
            self.EIGHT_BIT_RGB: 8,
            self.ELEVEN_BIT: 11
        }
        if self.pixel_type in bit_depth_map:
            return bit_depth_map[self.pixel_type]
        else:
            raise RuntimeError("Unknown pixel type")

    def as_byte_buffer(self):
        axes = {axis_name: position for axis_name, position in self.axes_key}
        axes_key_bytes = json.dumps(axes).encode('utf-8')
        filename_bytes = self.filename.encode('utf-8')

        buffer = BytesIO()
        buffer.write(struct.pack('I', len(axes_key_bytes)))
        buffer.write(axes_key_bytes)
        buffer.write(struct.pack('I', len(filename_bytes)))
        buffer.write(filename_bytes)
        buffer.write(struct.pack('IIIIIIII', self.pix_offset, self.image_width, self.image_height,
                                 self.pixel_type, self.pixel_compression, self.metadata_offset, self.metadata_length,
                                 self.metadata_compression))
        buffer.seek(0)
        return buffer

    @staticmethod
    def deserialize_axes(s):
        return json.loads(s)

    @staticmethod
    def serialize_axes(axes):
        return json.dumps(OrderedDict(sorted(axes.items())))
