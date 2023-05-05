"""
Library for reading NDTiff datasets
"""
import os
import numpy as np
import sys
import json
import dask.array as da
import warnings
import struct
import threading
from functools import partial
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

class NDTiffDataset():
    """
    Class that opens a single NDTiff dataset
    """

    def __init__(self, dataset_path=None, file_io: NDTiffFileIO = BUILTIN_FILE_IO, _summary_metadata=None,  **kwargs):
        """
        Provides access to an NDTiffStorage dataset,
        either one currently being acquired or one on disk

        Parameters
        ----------
        dataset_path : str
            Abosolute path of top level folder of a dataset on disk
        file_io: ndtiff.file_io.NDTiffFileIO
            A container containing various methods for interacting with files.
        _summary_metadata : dict
            Summary metadata for a dataset that is currently being acquired. Users shouldn't call this
        """
        self.file_io = file_io
        # if it is in fact a pyramid, the parent class will handle this. I think this implies that
        # resolution levels cannot be opened seperately and expected to stitch correctly when there
        # is tile overlap
        self._full_resolution = False
        self._lock = threading.RLock()
        if _summary_metadata is not None:
            # this dataset is a view of an active acquisition. Image data is being written by code on the Java side
            self._new_image_arrived = False # used by custom (e.g. napari) viewer to check for updates. Will be reset to false by them
            self.axes = {}
            self.axes_types = {}
            self.index = {}
            self._readers_by_filename = {}
            self._summary_metadata = _summary_metadata
            self.path = dataset_path
            self.path += "" if self.path[-1] == os.sep else os.sep
            return

        self.path = dataset_path
        self.path += "" if self.path[-1] == os.sep else os.sep
        self.index = self.read_index(self.path)
        tiff_names = [
            self.file_io.path_join(self.path, tiff) for tiff in self.file_io.listdir(self.path) if tiff.endswith(".tif")
        ]
        self._readers_by_filename = {}
        self.summary_metadata = {}
        self.major_version, self.minor_version = (0, 0)
        # Count how many files need to be opened
        num_tiffs = 0
        count = 0
        for file in self.file_io.listdir(self.path):
            if file.endswith(".tif"):
                num_tiffs += 1
        # populate list of readers and tree mapping indices to readers
        for tiff in tiff_names:
            print("\rOpening file {} of {}...".format(count + 1, num_tiffs), end="")
            count += 1
            new_reader = _SingleNDTiffReader(tiff, file_io=self.file_io)
            self._readers_by_filename[tiff.split(os.sep)[-1]] = new_reader
            # Should be the same on every file so resetting them is fine
            self.major_version, self.minor_version = new_reader.major_version, new_reader.minor_version

        if len(self._readers_by_filename) > 0:
            self.summary_metadata = list(self._readers_by_filename.values())[0].summary_md


        self.overlap = (
            np.array([
                    self.summary_metadata["GridPixelOverlapY"],
                    self.summary_metadata["GridPixelOverlapX"],
                ])
            if "GridPixelOverlapY" in self.summary_metadata
            else None
        )

        self.axes = {}
        self.axes_types = {}
        for axes_combo in self.index.keys():
            for axis, position in axes_combo:
                if axis not in self.axes.keys():
                    self.axes[axis] = []
                    self.axes_types[axis] = type(position)
                self.axes[axis].append(position)
                # get sorted unique elements
                self.axes[axis] = sorted(list(set(self.axes[axis])))
        # Sort axes according to _AXIS_ORDER
        self.axes = dict(sorted(self.axes.items(), key=_get_axis_order_key, reverse=True))

        # figure out the mapping of channel name to position by reading image metadata
        self._channels = {}
        self._parse_string_axes()

        # get information about image width and height, assuming that they are consistent for whole dataset
        # (which is not necessarily true but convenient when it is)
        self.bytes_per_pixel = 1
        self.dtype = np.uint8
        self.image_width, self.image_height = (0, 0)
        if len(self.index) > 0:
            with self._lock:
                first_index = list(self.index.values())[0]
            self._parse_first_index(first_index)

        print("\rDataset opened                ")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    def get_channel_names(self):
        """
        :return: list of channel names (strings)
        """
        return list(self._channels.keys())

    def has_new_image(self):
        """
        For datasets currently being acquired, check whether a new image has arrived since this function
        was last called, so that a viewer displaying the data can be updated.
        """
        # pass through to full resolution, since only this is monitored in current implementation
        if not hasattr(self, '_new_image_arrived'):
            return False # pre-initilization
        new = self._new_image_arrived
        self._new_image_arrived = False
        return new

    def has_image(
        self,
        channel=None,
        z=None,
        time=None,
        position=None,
        row=None,
        column=None,
        **kwargs
    ):
        """Check if this image is present in the dataset

        Parameters
        ----------
        channel : int or str
            index of the channel, if applicable (Default value = None)
        z : int
            index of z slice, if applicable (Default value = None)
        time : int
            index of the time point, if applicable (Default value = None)
        position : int
            index of the XY position, if applicable (Default value = None)
        row : int
            index of tile row for XY tiled datasets (Default value = None)
        column : int
            index of tile column for XY tiled datasets (Default value = None)
        **kwargs :
            names and integer positions of any other axes

        Returns
        -------
        bool :
            indicating whether the dataset has an image matching the specifications
        """
        with self._lock:
            return self._does_have_image(self._consolidate_axes(
                channel, z, position, time, row, column, **kwargs))

    def read_image(
        self,
        channel=None,
        z=None,
        time=None,
        position=None,
        row=None,
        column=None,
        **kwargs
    ):
        """
        Read image data as numpy array

        Parameters
        ----------
        channel : int or str
            index of the channel, if applicable (Default value = None)
        z : int
            index of z slice, if applicable (Default value = None)
        time : int
            index of the time point, if applicable (Default value = None)
        position : int
            index of the XY position, if applicable (Default value = None)
        row : int
            index of tile row for XY tiled datasets (Default value = None)
        column : int
            index of tile column for XY tiled datasets (Default value = None)
        **kwargs :
            names and integer positions of any other axes

        Returns
        -------
        image : numpy array or tuple
            image as a 2D numpy array, or tuple with image and image metadata as dict

        """
        with self._lock:
            axes = self._consolidate_axes(channel, z, position, time, row, column, **kwargs )

            return self._do_read_image(axes)

    def read_metadata(
        self,
        channel=None,
        z=None,
        time=None,
        position=None,
        row=None,
        column=None,
        **kwargs
    ):
        """
        Read metadata only. Faster than using read_image to retrieve metadata

        Parameters
        ----------
        channel : int or str
            index of the channel, if applicable (Default value = None)
        z : int
            index of z slice, if applicable (Default value = None)
        time : int
            index of the time point, if applicable (Default value = None)
        position : int
            index of the XY position, if applicable (Default value = None)
        row : int
            index of tile row for XY tiled datasets (Default value = None)
        column : int
            index of tile col for XY tiled datasets (Default value = None)

        **kwargs :
            names and integer positions of any other axes

        Returns
        -------
        metadata : dict

        """
        with self._lock:
            axes = self._consolidate_axes(
                channel, z, position, time, row, column, **kwargs
            )

            return self._do_read_metadata(axes)



    def get_index_keys(self):
        """
        Return a list of every combination of axes that has a image in this dataset
        """
        frozen_set_list = list(self.index.keys())
        # convert to dict
        return [{axis_name: position for axis_name, position in key} for key in frozen_set_list]

    def _add_index_entry(self, data):
        """
        Add entry for a image that has been recieved and is now on disk
        This is used to get access to a dataset that is currently being
        written by java side
        """
        with self._lock:
            _, axes, index_entry = self._read_single_index_entry(data, self.index)

            if index_entry["filename"] not in self._readers_by_filename:
                new_reader = _SingleNDTiffReader(self.path + index_entry["filename"], file_io=self.file_io)
                self._readers_by_filename[index_entry["filename"]] = new_reader
                # Should be the same on every file so resetting them is fine
                self.major_version, self.minor_version = new_reader.major_version, new_reader.minor_version


            # update the axes that have been seen
            for axis_name in axes.keys():
                if axis_name not in self.axes.keys():
                    self.axes[axis_name] = []
                    self.axes_types[axis_name] = type(axes[axis_name])
                self.axes[axis_name].append(axes[axis_name])
                self.axes[axis_name] = sorted(list(set(self.axes[axis_name])))

            # update the map of channel names to channel indices
            self._parse_string_axes()

        if not hasattr(self, 'image_width'):
            self._parse_first_index(index_entry)

        return axes

    def _consolidate_axes(self, channel: int or str, z: int, position: int,
                          time: int, row: int, column: int, **kwargs):
        """
        Pack axes into a convenient format
        """
        if ('channel_name' in kwargs):
            warnings.warn('channel_name is deprecated, use "channel" instead')
            channel = kwargs['channel_name']
            del kwargs['channel_name']

        axis_positions = {'channel': channel, 'z': z, 'position': position,
                    'time': time, 'row': row, 'column': column, **kwargs}
        # ignore ones that are None
        axis_positions = {n: axis_positions[n] for n in axis_positions.keys() if axis_positions[n] is not None}
        for axis_name in axis_positions.keys():
            # convert any string-valued axes passed as ints into strings
            if self.axes_types[axis_name] == str and type(axis_positions[axis_name]) == int:
                axis_positions[axis_name] = self._string_axes_values[axis_name][axis_positions[axis_name]]

        return axis_positions

    def _parse_string_axes(self):
        """
        As of NDTiff 3.2, axes are allowed to take string values: e.g. {'channel': 'DAPI'}
        This is allowed on any axis. This function returns a tuple of possible values along
        the string axis in order to be able to interconvert integer values and string values.
        """
        # iterate through the key_combos for each image
        if self.major_version >= 3 and self.minor_version >= 2:
            self._string_axes_values = {axis_name: [] for axis_name in self.axes_types.keys()
                                        if self.axes_types[axis_name] is str}
            for key in self.index.keys():
                for axis_name, position in key:
                    if axis_name in self._string_axes_values.keys() and \
                            position not in self._string_axes_values[axis_name]:
                        self._string_axes_values[axis_name].append(position)
            if _CHANNEL_AXIS in self._string_axes_values:
                self._channels = {name: self._string_axes_values[_CHANNEL_AXIS].index(name)
                                  for name in self._string_axes_values[_CHANNEL_AXIS]}
        else:
            # before string-valued axes were allowed in NDTiff 3.1
            if 'ChNames' in self.summary_metadata:
                # It was created by a MM MDA/Clojure acquistiion engine
                self._channels = {name: i for i, name in enumerate(self.summary_metadata['ChNames'])}
            else:
                # AcqEngJ
                if _CHANNEL_AXIS in self.axes.keys():
                    for key in self.index.keys():
                        axes = {axis: position for axis, position in key}
                        if (
                            _CHANNEL_AXIS in axes.keys()
                            and axes[_CHANNEL_AXIS] not in self._channels.values()
                        ):
                            channel_name = self.read_metadata(**axes)["Channel"]
                            self._channels[channel_name] = axes[_CHANNEL_AXIS]
                        if len(self._channels.values()) == len(self.axes[_CHANNEL_AXIS]):
                            break


    def _parse_first_index(self, first_index):
        """
        Read through first index to get some global data about images (assuming it is same for all)
        """
        if first_index["pixel_type"] == _SingleNDTiffReader.EIGHT_BIT_RGB:
            self.bytes_per_pixel = 3
            self.dtype = np.uint8
        elif first_index["pixel_type"] == _SingleNDTiffReader.EIGHT_BIT_MONOCHROME:
            self.bytes_per_pixel = 1
            self.dtype = np.uint8
        elif first_index["pixel_type"] == _SingleNDTiffReader.SIXTEEN_BIT_MONOCHROME or \
                first_index["pixel_type"] == _SingleNDTiffReader.FOURTEEN_BIT_MONOCHROME or \
                first_index["pixel_type"] == _SingleNDTiffReader.TWELVE_BIT_MONOCHROME or \
                first_index["pixel_type"] == _SingleNDTiffReader.TEN_BIT_MONOCHROME or \
                first_index["pixel_type"] == _SingleNDTiffReader.ELEVEN_BIT_MONOCHROME:
            self.bytes_per_pixel = 2
            self.dtype = np.uint16

        self.image_width = first_index["image_width"]
        self.image_height = first_index["image_height"]

    def _does_have_image(self, axes):
        key = frozenset(axes.items())
        return key in self.index

    def _read_single_index_entry(self, data, entries, position=0):
        index_entry = {}
        (axes_length,) = struct.unpack("I", data[position : position + 4])
        if axes_length == 0:
            warnings.warn(
                "Index appears to not have been properly terminated (the dataset may still work)"
            )
            return None
        axes_str = data[position + 4 : position + 4 + axes_length].decode("utf-8")
        axes = json.loads(axes_str)
        position += axes_length + 4
        (filename_length,) = struct.unpack("I", data[position : position + 4])
        index_entry["filename"] = data[position + 4 : position + 4 + filename_length].decode(
            "utf-8"
        )
        position += 4 + filename_length
        (
            index_entry["pixel_offset"],
            index_entry["image_width"],
            index_entry["image_height"],
            index_entry["pixel_type"],
            index_entry["pixel_compression"],
            index_entry["metadata_offset"],
            index_entry["metadata_length"],
            index_entry["metadata_compression"],
        ) = struct.unpack("IIIIIIII", data[position : position + 32])
        position += 32
        entries[frozenset(axes.items())] = index_entry
        return position, axes, index_entry

    def read_index(self, path):
        print("\rReading index...          ", end="")
        with self.file_io.open(path + os.sep + "NDTiff.index", "rb") as index_file:
            data = index_file.read()
        entries = {}
        position = 0
        while position < len(data):
            print(
                "\rReading index... {:.1f}%       ".format(
                    100 * (1 - (len(data) - position) / len(data))
                ),
                end="",
            )
            entry = self._read_single_index_entry(data, entries, position)
            if entry is None:
                break
            position, axes, index_entry = entry
            if position is None:
                break

        print("\rFinshed reading index          ", end="")
        return entries

    def _do_read_image(
        self,
        axes,
    ):
        # determine which reader contains the image
        key = frozenset(axes.items())
        if key not in self.index:
            raise Exception("image with keys {} not present in data set".format(key))
        index = self.index[key]
        reader = self._readers_by_filename[index["filename"]]
        return reader.read_image(index)

    def _do_read_metadata(self, axes):
        """

        Parameters
        ----------
        axes : dict

        Returns
        -------
        image_metadata
        """
        key = frozenset(axes.items())
        if key not in self.index:
            raise Exception("image with keys {} not present in data set".format(key))
        index = self.index[key]
        reader = self._readers_by_filename[index["filename"]]
        return reader.read_metadata(index)

    def close(self):
        for reader in self._readers_by_filename.values():
            reader.close()

    def _read_one_image(self, block_id, axes_to_stack=None, axes_to_slice=None, stitched=False, rgb=False):
        # a function that reads in one chunk of data
        axes = {key: block_id[i] for i, key in enumerate(axes_to_stack.keys())}
        if stitched:
            # Combine all rows and cols into one stitched image
            # get spatial layout of position indices
            row_values = np.array(list(self.axes["row"]))
            column_values = np.array(list(self.axes["column"]))
            # fill in missing values
            row_values = np.arange(np.min(row_values), np.max(row_values) + 1)
            column_values = np.arange(np.min(column_values), np.max(column_values) + 1)
            # make nested list of rows and columns
            blocks = []
            for row in row_values:
                blocks.append([])
                for column in column_values:
                    # remove overlap between tiles
                    if not self.has_image(**axes, **axes_to_slice, row=row, column=column):
                        blocks[-1].append(self._empty_tile)
                    else:
                        tile = self.read_image(**axes, **axes_to_slice, row=row, column=column)
                        # remove half of the overlap around each tile so that that image stitches correctly
                        # only need this for full resoution because downsampled ones already have the edges removed
                        if np.any(self.overlap[0] > 0) and self._full_resolution:
                            min_index = np.floor(self.overlap / 2).astype(np.int_)
                            max_index = np.ceil(self.overlap / 2).astype(np.int_)
                            tile = tile[min_index[0]:-max_index[0], min_index[1]:-max_index[1]]
                        blocks[-1].append(tile)

            if rgb:
                image = np.concatenate([np.concatenate(row, axis=len(blocks[0][0].shape) - 2)
                        for row in blocks],  axis=0)
            else:
                image = np.array(da.block(blocks))
        else:
            if not self.has_image(**axes, **axes_to_slice):
                image = self._empty_tile
            else:
                image = self.read_image(**axes, **axes_to_slice)
        for i in range(len(axes_to_stack.keys())):
            image = image[None]
        return image

    def as_array(self, axes=None, stitched=False, **kwargs):
        """
        Read all data image data as one big Dask array with last two axes as y, x and preceeding axes depending on data.
        The dask array is made up of memory-mapped numpy arrays, so the dataset does not need to be able to fit into RAM.
        If the data doesn't fully fill out the array (e.g. not every z-slice collected at every time point), zeros will
        be added automatically.

        To convert data into a numpy array, call np.asarray() on the returned result. However, doing so will bring the
        data into RAM, so it may be better to do this on only a slice of the array at a time.

        Parameters
        ----------
        axes : list
            list of axes names over which to iterate and merge into a stacked array. The order of axes supplied in this 
            list will be the order of the axes of the returned dask array. If None, all axes will be used in PTCZYX order.
        stitched : bool
            If true and tiles were acquired in a grid, lay out adjacent tiles next to one another (Default value = False)
        **kwargs :
            names and integer positions of axes on which to slice data
        Returns
        -------
        dataset : dask array
        """
        if stitched and "GridPixelOverlapX" not in self.summary_metadata:
            raise Exception('This is not a stitchable dataset')
        if not stitched or not self._full_resolution:
            w = self.image_width
            h = self.image_height
        elif self._full_resolution:
            w = self.image_width - self.overlap[1]
            h = self.image_height - self.overlap[0]

        self._empty_tile = (
            np.zeros((h, w), self.dtype)
            if self.bytes_per_pixel != 3
            else np.zeros((h, w, 3), self.dtype)
        )

        rgb = self.bytes_per_pixel == 3 and self.dtype == np.uint8

        if axes is None:
            axes = self.axes.keys()
        axes_to_slice = kwargs
        axes_to_stack = {key: self.axes[key] for key in axes if key not in kwargs.keys()}
        if stitched:
            if 'row' in axes_to_stack:
                del axes_to_stack['row']
            if 'column' in axes_to_stack:
                del axes_to_stack['column']
            if 'row' in axes_to_slice:
                del axes_to_slice['row']
            if 'column' in axes_to_slice:
                del axes_to_slice['column']

        chunks = tuple([(1,) * len(axes_to_stack[axis]) for axis in axes_to_stack.keys()])
        if stitched:
            row_values = np.array(list(self.axes["row"]))
            column_values = np.array(list(self.axes["column"]))
            chunks += (h * (np.max(row_values) - np.min(row_values) + 1),
                       w * (np.max(column_values) - np.min(column_values) + 1))
        else:
            chunks += (h, w)
        if rgb:
            chunks += (3,)

        array = da.map_blocks(
            partial(self._read_one_image, axes_to_stack=axes_to_stack, axes_to_slice=axes_to_slice, stitched=stitched, rgb=rgb),
            dtype=self.dtype,
            chunks=chunks,
            meta=self._empty_tile
        )

        return array


class NDTiffPyramidDataset():
    """Class that opens a single NDTiffStorage multi-resolution pyramid dataset"""

    def __init__(self, dataset_path=None, file_io: NDTiffFileIO = BUILTIN_FILE_IO, _summary_metadata=None):
        """
        Provides access to a NDTiffStorage pyramid dataset,
        either one currently being acquired or one on disk

        Parameters
        ----------
        dataset_path : str
            Abosolute path of top level folder of a dataset on disk
        file_io: ndtiff.file_io.NDTiffFileIO
            A container containing various methods for interacting with files.
        _summary_metadata : dict
            Summary metadata, only not None for in progress datasets. Users need not call directly
        """
        self.file_io = file_io
        self._lock = threading.RLock()
        if _summary_metadata is not None:
            # this dataset is a view of an active acquisition. The storage exists on the java side
            self.path = dataset_path
            self.path += "" if self.path[-1] == os.sep else os.sep
            self.summary_metadata = _summary_metadata

            with self._lock:
                full_res = NDTiffDataset(dataset_path=self.path + "Full resolution" + os.sep,
                                         _summary_metadata=_summary_metadata, file_io=file_io)
                self.res_levels = {0: full_res}
                full_res._full_resolution = True
            # No information related higher res levels when remote storage monitoring right now

            #Copy stuff from the full res class for convenience
            self.axes = self.res_levels[0].axes

            self.overlap = (np.array([self.summary_metadata["GridPixelOverlapY"],
                                        self.summary_metadata["GridPixelOverlapX"] ]))
            self.res_levels[0].overlap = self.overlap
            # TODO maybe open other resoutions here too
            return

        # Loading from disk
        self.path = dataset_path
        self.path += "" if self.path[-1] == os.sep else os.sep
        res_dirs = [
            dI for dI in self.file_io.listdir(dataset_path) if self.file_io.isdir(self.file_io.path_join(dataset_path, dI))
        ]
        # map from downsample factor to dataset
        with self._lock:
            self.res_levels = {}
        if "Full resolution" not in res_dirs:
            #Probably won't happen because this was already checked for
            raise Exception(
                "Couldn't find full resolution directory. Is this the correct path to a dataset?"
            )

        for res_dir in res_dirs:
            res_dir_path = self.file_io.path_join(dataset_path, res_dir)
            res_level = NDTiffDataset(dataset_path=res_dir_path, file_io=self.file_io)
            if res_dir == "Full resolution":
                with self._lock:
                    self.res_levels[0] = res_level
                res_level._full_resolution = True
                # get summary metadata and index tree from full resolution image
                self.summary_metadata = res_level.summary_metadata

                self.overlap = (
                    np.array([
                            self.summary_metadata["GridPixelOverlapY"],
                            self.summary_metadata["GridPixelOverlapX"],
                        ])
                )

                self.axes = res_level.axes
                self.bytes_per_pixel = res_level.bytes_per_pixel
                self.dtype = res_level.dtype
                self.image_width = res_level.image_width
                self.image_height = res_level.image_height

            else:
                res_level._full_resolution = False
                with self._lock:
                    self.res_levels[int(np.log2(int(res_dir.split("x")[1])))] = res_level

        print("\rDataset Pyramid opened                ")


    def get_index_keys(self, res_level=0):
        """
        Return a list of every combination of axes that has a image in this dataset
        """
        return self.res_levels[res_level].get_index_keys()

    def has_new_image(self):
        """
        For datasets currently being acquired, check whether a new image has arrived since this function
        was last called, so that a viewer displaying the data can be updated.
        """
        # pass through to full resolution, since only this is monitored in current implementation
        return self.res_levels[0]

    def as_array(self, axes=None, stitched=False, res_level=None, **kwargs):
        """
        Read all data image data as one big Dask array with last two axes as y, x and preceeding axes depending on data.
        The dask array is made up of memory-mapped numpy arrays, so the dataset does not need to be able to fit into RAM.
        If the data doesn't fully fill out the array (e.g. not every z-slice collected at every time point), zeros will
        be added automatically.

        To convert data into a numpy array, call np.asarray() on the returned result. However, doing so will bring the
        data into RAM, so it may be better to do this on only a slice of the array at a time.

        Parameters
        ----------
        axes : list
            list of axes names over which to iterate and merge into a stacked array. If None, all axes will be used.
            The order of axes supplied in this list will be the order of the axes of the returned dask array
        stitched : bool
            Lay out adjacent tiles next to one another to form a larger image (Default value = False)
        res_level : int or None
            the resolution level to return. If None, return all resolutions in a list
        **kwargs :
            names and integer positions of axes on which to slice data
        Returns
        -------
        dataset : dask array
        """
        tile_shape = np.array([self.image_height, self.image_width]) - self.overlap


        def get_tile_index_from_pixel_index(pixel_index):
            """
            :param pixel_index: pixel index at relevant resolution
            """
            negative_mask = pixel_index < 0
            # positive
            tile_index = pixel_index // tile_shape
            # negative
            tile_index[negative_mask] = (-1 - (np.abs(1 + pixel_index) // tile_shape))[negative_mask]
            return tile_index


        if res_level is not None:
            return self.res_levels[res_level].as_array(axes=axes, stitched=stitched, **kwargs)
        else:
            row_values = np.array(list(self.axes["row"]))
            column_values = np.array(list(self.axes["column"]))
            pixel_extent_min = np.array([np.min(row_values), np.min(column_values)]) * tile_shape
            pixel_extent_max = np.array([np.max(row_values) + 1, np.max(column_values) + 1]) * tile_shape
            images = []
            for res_level in set(self.res_levels.keys()):
                if res_level == 0:
                    image = self.res_levels[res_level].as_array(axes=axes, stitched=stitched, **kwargs)
                    images.append(image)
                else:
                    image = self.res_levels[res_level].as_array(axes=axes, stitched=stitched, **kwargs)
                    # crop away zero padding that extends pass where data is collected
                    res_level_pixel_extent_min = (pixel_extent_min / 2 ** res_level).astype(np.int_)
                    res_level_pixel_extent_max = (pixel_extent_max / 2 ** res_level).astype(np.int_)
                    if np.min(np.stack([res_level_pixel_extent_max - res_level_pixel_extent_min,
                                        res_level_pixel_extent_max - res_level_pixel_extent_min])) < 16:
                        # Not worth it to use ones this small
                        break
                    # Subtract one to get pixel index from max extent because min extent is inclusive but max exclusive
                    min_tile_index = get_tile_index_from_pixel_index(res_level_pixel_extent_min)
                    max_tile_index = get_tile_index_from_pixel_index(res_level_pixel_extent_max - 1)
                    # get the pixel coordinates of the tiles that contain the data
                    res_level_container_extent_min = min_tile_index * tile_shape
                    res_level_container_extent_max = (max_tile_index + 1) * tile_shape
                    offset = res_level_pixel_extent_min - res_level_container_extent_min
                    extent = res_level_pixel_extent_max - res_level_pixel_extent_min
                    image = image[..., offset[0]: offset[0] + extent[0],
                            offset[1]: offset[1] + extent[1]]
                    images.append(image)


            return images


    def has_image(
        self,
        channel=None,
        z=None,
        time=None,
        position=None,
        resolution_level=0,
        row=None,
        column=None,
        **kwargs
    ):
        """Check if this image is present in the dataset

        Parameters
        ----------
        channel : int
            index of the channel, if applicable (Default value = None)
        z : int
            index of z slice, if applicable (Default value = None)
        time : int
            index of the time point, if applicable (Default value = None)
        position : int
            index of the XY position, if applicable (Default value = None)
        row : int
            index of tile row for XY tiled datasets (Default value = None)
        column : int
            index of tile column for XY tiled datasets (Default value = None)
        resolution_level :
            0 is full resolution, otherwise represents downampling of pixels
            at 2 ** (resolution_level) (Default value = 0)
        **kwargs :
            names and integer positions of any other axes

        Returns
        -------
        bool :
            indicating whether the dataset has an image matching the specifications
        """
        with self._lock:
            return self.res_levels[resolution_level].has_image(
                channel=channel,
                z=z,
                time=time,
                position=position,
                row=row,
                column=column,
                **kwargs
            )

    def read_image(
        self,
        channel=None,
        z=None,
        time=None,
        position=None,
        row=None,
        column=None,
        resolution_level=0,
        **kwargs
    ):
        """
        Read image data as numpy array

        Parameters
        ----------
        channel : int
            index of the channel, if applicable (Default value = None)
        z : int
            index of z slice, if applicable (Default value = None)
        time : int
            index of the time point, if applicable (Default value = None)
        position : int
            index of the XY position, if applicable (Default value = None)
        row : int
            index of tile row for XY tiled datasets (Default value = None)
        column : int
            index of tile col for XY tiled datasets (Default value = None)
        resolution_level :
            0 is full resolution, otherwise represents downampling of pixels
            at 2 ** (resolution_level) (Default value = 0)
        **kwargs :
            names and integer positions of any other axes

        Returns
        -------
        image : numpy array or tuple
            image as a 2D numpy array, or tuple with image and image metadata as dict

        """
        with self._lock:
            return self.res_levels[resolution_level].read_image(channel=channel,
                    z=z,
                    time=time,
                    position=position,
                    row=row,
                    column=column,
                    **kwargs)

    def read_metadata(
        self,
        channel=None,
        z=None,
        time=None,
        position=None,
        row=None,
        column=None,
        resolution_level=0,
        **kwargs
    ):
        """
        Read metadata only. Faster than using read_image to retrieve metadata

        Parameters
        ----------
        channel : int
            index of the channel, if applicable (Default value = None)
        z : int
            index of z slice, if applicable (Default value = None)
        time : int
            index of the time point, if applicable (Default value = None)
        position : int
            index of the XY position, if applicable (Default value = None)
        row : int
            index of tile row for XY tiled datasets (Default value = None)
        column : int
            index of tile col for XY tiled datasets (Default value = None)
        resolution_level :
            0 is full resolution, otherwise represents downampling of pixels
            at 2 ** (resolution_level) (Default value = 0)
        **kwargs :
            names and integer positions of any other axes

        Returns
        -------
        metadata : dict

        """
        with self._lock:
            return self.res_levels[resolution_level].read_metadata(
                    channel=channel,
                    z=z,
                    time=time,
                    position=position,
                    row=row,
                    column=column,
                    **kwargs)

    def _add_index_entry(self, index_entry):
        # Pass through to full res data
        return self.res_levels[0]._add_index_entry(index_entry)

    def close(self):
        with self._lock:
            for res_level in self.res_levels:
                res_level.close()
