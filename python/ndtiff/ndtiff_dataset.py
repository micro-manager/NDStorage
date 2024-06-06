import os
import threading
import json
import struct
import warnings
import dask.array as da
from sortedcontainers import SortedSet
from functools import partial

from ndtiff.file_io import NDTiffFileIO, BUILTIN_FILE_IO
from ndtiff.ndtiff_file import _SingleNDTiffReader
from ndtiff.ndtiff_file import _POSITION_AXIS, _ROW_AXIS, _COLUMN_AXIS, _Z_AXIS, _TIME_AXIS, _CHANNEL_AXIS, _AXIS_ORDER, _get_axis_order_key
from ndtiff.ndtiff_index import NDTiffIndexEntry, read_ndtiff_index

class NDTiffDataset:
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
        print("\rReading index...          ", end="")
        with self.file_io.open(self.path + "NDTiff.index", "rb") as index_file:
            data = index_file.read()
            self.index = read_ndtiff_index(data)

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
                    self.axes[axis] = SortedSet()
                    self.axes_types[axis] = type(position)
                self.axes[axis].add(position)
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
            _, axes, index_entry = self.read_single_index_entry(data)
            self.index[frozenset(axes.items())] = index_entry

            if index_entry["filename"] not in self._readers_by_filename:
                new_reader = _SingleNDTiffReader(self.path + index_entry["filename"], file_io=self.file_io)
                self._readers_by_filename[index_entry["filename"]] = new_reader
                # Should be the same on every file so resetting them is fine
                self.major_version, self.minor_version = new_reader.major_version, new_reader.minor_version


            # update the axes that have been seen
            for axis_name in axes.keys():
                if axis_name not in self.axes.keys():
                    self.axes[axis_name] = SortedSet()
                    self.axes_types[axis_name] = type(axes[axis_name])
                self.axes[axis_name].add(axes[axis_name])

            # update the map of channel names to channel indices
            self._parse_string_axes(axes)

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

    def _parse_string_axes(self, single_axes_position=None):
        """
        As of NDTiff 3.2, axes are allowed to take string values: e.g. {'channel': 'DAPI'}
        This is allowed on any axis. This function returns a tuple of possible values along
        the string axis in order to be able to interconvert integer values and string values.

        param single_axes_position: if provided, only parse this newly added entry
        """
        # iterate through the key_combos for each image
        if self.major_version >= 3 and self.minor_version >= 2:
            # keep track of the values for each axis with string values if not doing so already
            if not hasattr(self, '_string_axes_values'):
                self._string_axes_values = {}
            # if this is a new axis_value for a string axis, add a list to populate
            for string_axis_name in [axis_name for axis_name in self.axes_types.keys() if self.axes_types[axis_name] is str]:
                if string_axis_name not in self._string_axes_values.keys():
                    self._string_axes_values[string_axis_name] = []

            # add new axis values to the list of values that have been seen
            if single_axes_position is None:
                # parse all axes_positions in the dataset
                for single_axes_position in self.index.keys():
                    # this is a set of tuples of (axis_name, axis_value)
                    for axis_name, axis_value in single_axes_position:
                        if axis_name in self._string_axes_values.keys() and \
                                axis_value not in self._string_axes_values[axis_name]:
                            self._string_axes_values[axis_name].append(axis_value)
            else:
                # parse only this set of axes positions
                for axis_name, axis_value in single_axes_position.items():
                    if axis_name in self._string_axes_values.keys() and \
                            axis_value not in self._string_axes_values[axis_name]:
                        self._string_axes_values[axis_name].append(axis_value)

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
                        single_axes_position = {axis: position for axis, position in key}
                        if (
                            _CHANNEL_AXIS in single_axes_position.keys()
                            and single_axes_position[_CHANNEL_AXIS] not in self._channels.values()
                        ):
                            channel_name = self.read_metadata(**single_axes_position)["Channel"]
                            self._channels[channel_name] = single_axes_position[_CHANNEL_AXIS]
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
        axes = {key: axes_to_stack[key][block_id[i]] for i, key in enumerate(axes_to_stack.keys())}
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
        axes_to_stack = {key: list(self.axes[key]) for key in axes if key not in kwargs.keys()}
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
