"""
Abstract base class for ND image storage reading and writing

Terminiology:
- axis: a dimension of the data, e.g. 'channel', 'z', 'time'
- position: the index of a particular axis, e.g. 0, 1, 2, 3
- image_coordinates: a dictionary of axis names and their positions, e.g. {'channel': 0, 'z': 1, 'time': 2}
- image_key: a frozenset of image_coordinates, e.g. frozenset({'channel': 0, 'z': 1, 'time': 2})
- image_keys: a list of image_keys, e.g. [frozenset({'channel': 0, 'z': 1, 'time': 2}), frozenset({'channel': 0, 'z': 1, 'time': 3})]
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Union, Tuple
import numpy as np
import dask
import warnings
from sortedcontainers import SortedSet
from functools import partial
import dask.array as da
import threading

from ndstorage.ndtiff_file import _POSITION_AXIS, _ROW_AXIS, _COLUMN_AXIS, _Z_AXIS, _TIME_AXIS, _CHANNEL_AXIS

# Standard order of axes when opening data. Data does not need to be acquired in this order -- this is just a convention
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

class NDStorageAPI(ABC):
    """
    API for NDStorage classes
    """
    def __init__(self):
        # a dictionary of the types (int or str) of each axis
        # {'channel': str, 'z': int, 'time': int}
        self.axes_types = {}

        # a list of all the axes that have been seen with their values in order
        # e.g. {'time': SortedSet([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])}
        self.axes = {}

        # Metadata that applies to the entire dataset
        self.summary_metadata = None

    @abstractmethod
    def has_image(self, channel: Union[int, str] = None, z: int = None, time: int = None,
                  position: int = None, row: int = None, column: int = None,
                  **kwargs: Union[int, str]) -> bool:
        """
        Check if this image is present in the dataset.

        Parameters
        ----------
        channel : int or str, optional
            Integer index or string name of the channel (Default value = None)
        z : int, optional
            Index of z slice (Default value = None)
        time : int, optional
            Index of the time point (Default value = None)
        position : int, optional
            Index of the XY position (Default value = None)
        row : int, optional
            Index of tile row for XY tiled datasets (Default value = None)
        column : int, optional
            Index of tile column for XY tiled datasets (Default value = None)
        **kwargs :
            Names and integer positions of any other axes

        Returns
        -------
        bool :
            Indicates whether the dataset has an image matching the specifications
        """
        pass

    @abstractmethod
    def read_image(self, channel: Union[int, str] = None, z: int = None, time: int = None,
                   position: int = None, row: int = None, column: int = None,
                   **kwargs: Union[int, str]) -> Union[np.ndarray, Tuple[np.ndarray, Dict[str, Any]]]:
        """
        Read image data as numpy array.

        Parameters
        ----------
        channel : int or str, optional
            Integer index or string name of the channel (Default value = None)
        z : int, optional
            Index of z slice (Default value = None)
        time : int, optional
            Index of the time point (Default value = None)
        position : int, optional
            Index of the XY position (Default value = None)
        row : int, optional
            Index of tile row for XY tiled datasets (Default value = None)
        column : int, optional
            Index of tile column for XY tiled datasets (Default value = None)
        **kwargs :
            Names and int or str positions of any other axes

        Returns
        -------
        image : numpy array
            Image as a 2D numpy array

        """
        pass

    @abstractmethod
    def read_metadata(self, channel: Union[int, str] = None, z: int = None, time: int = None,
                      position: int = None, row: int = None, column: int = None,
                      **kwargs: Union[int, str]) -> Dict[str, Any]:
        """
        Read metadata only.

        Parameters
        ----------
        channel : int or str, optional
            Integer index or string name of the channel (Default value = None)
        z : int, optional
            Index of z slice (Default value = None)
        time : int, optional
            Index of the time point (Default value = None)
        position : int, optional
            Index of the XY position (Default value = None)
        row : int, optional
            Index of tile row for XY tiled datasets (Default value = None)
        column : int, optional
            Index of tile col for XY tiled datasets (Default value = None)
        **kwargs :
            Names and integer positions of any other axes

        Returns
        -------
        metadata : dict
            Image metadata

        """
        pass

    @abstractmethod
    def close(self):
        """
        Close the dataset, releasing any resources it holds
        """
        pass

    @abstractmethod
    def get_image_coordinates_list(self) -> List[Dict[str, Union[int, str]]]:
        """
        Return a list of the coordinates (e.g. {'channel': 'DAPI', 'z': 0, 'time': 0}) of every image in the dataset

        Returns
        -------
        list
            List of image coordinates
        """
        pass

    @abstractmethod
    def await_new_image(self, timeout=None):
        """
        Wait for a new image to arrive in the dataset

        Parameters
        ----------
        timeout : float, optional
            Maximum time to wait in seconds (Default value = None)

        Returns
        -------
        bool
            True if a new image has arrived, False if the timeout was reached
        """
        pass

    @abstractmethod
    def is_finished(self) -> bool:
        """
        Check if the dataset is finished and no more images will be added
        """
        pass



    @abstractmethod
    def as_array(self, axes: List[str] = None, stitched: bool = False,
                 **kwargs: Union[int, str]) -> 'dask.array':
        """
        Create one big Dask array with last two axes as y, x and preceding axes depending on data.
        If the dataset is saved to disk, the dask array is made up of memory-mapped numpy arrays,
        so the dataset does not need to be able to fit into RAM.
        If the data doesn't fully fill out the array (e.g. not every z-slice collected at every time point),
        zeros will be added automatically.

        To convert data into a numpy array, call np.asarray() on the returned result. However, doing so will bring the
        data into RAM, so it may be better to do this on only a slice of the array at a time.

        Parameters
        ----------
        axes : list, optional
            List of axes names over which to iterate and merge into a stacked array.
            If None, all axes will be used in PTCZYX order (Default value = None).
        stitched : bool, optional
            If True and tiles were acquired in a grid, lay out adjacent tiles next to one another
            (Default value = False)
        **kwargs :
            Names and integer positions of axes on which to slice data

        Returns
        -------
        dataset : dask array
            Dask array representing the dataset
        """
        pass

class WritableNDStorageAPI(NDStorageAPI):
    """
    API for NDStorage classes to which images can be written
    """

    @abstractmethod
    def put_image(self, coordinates, image, metadata):
        """
        Write an image to the dataset

        Parameters
        ----------
        coordinates : dict
            dictionary of axis names and positions
        image : numpy array
            image data
        metadata : dict
            image metadata

        """
        pass

    @abstractmethod
    def finish(self):
        """
        No more images will be written to the dataset
        """
        pass

    @abstractmethod
    def initialize(self, summary_metadata: dict):
        """
        Initialize the dataset with summary metadata
        """
        pass

    @abstractmethod
    def block_until_finished(self, timeout=None):
        """
        Block until the dataset is finished and all images have been written

        Parameters
        ----------
        timeout : float, optional
            Maximum time to wait in seconds (Default value = None)

        Returns
        -------
        bool
            True if the dataset is finished, False if the timeout was reached
        """
        pass


class NDStorageBase(NDStorageAPI):
    """
    Base class with helpful methods for reading and writing ND data
    """
    def __init__(self):
        super().__init__()
        self._string_axes_values = {}

        self.image_width = None
        self.image_height = None
        self.dtype = None
        self.bytes_per_pixel = None

        # for stitched datasets
        self._overlap = None
        self._full_resolution = None

        self._new_image_event = threading.Event()

    @abstractmethod
    def has_image(self, channel: Union[int, str] = None, z: int = None, time: int = None,
                  position: int = None, row: int = None, column: int = None,
                  **kwargs: Union[int, str]) -> bool:
        """
        Check if this image is present in the dataset.

        Parameters
        ----------
        channel : int or str, optional
            Integer index or string name of the channel (Default value = None)
        z : int, optional
            Index of z slice (Default value = None)
        time : int, optional
            Index of the time point (Default value = None)
        position : int, optional
            Index of the XY position (Default value = None)
        row : int, optional
            Index of tile row for XY tiled datasets (Default value = None)
        column : int, optional
            Index of tile column for XY tiled datasets (Default value = None)
        **kwargs :
            Names and integer positions of any other axes

        Returns
        -------
        bool :
            Indicates whether the dataset has an image matching the specifications
        """
        pass

    @abstractmethod
    def read_image(self, channel: Union[int, str] = None, z: int = None, time: int = None,
                   position: int = None, row: int = None, column: int = None,
                   **kwargs: Union[int, str]) -> Union[np.ndarray, Tuple[np.ndarray, Dict[str, Any]]]:
        """
        Read image data as numpy array.

        Parameters
        ----------
        channel : int or str, optional
            Integer index or string name of the channel (Default value = None)
        z : int, optional
            Index of z slice (Default value = None)
        time : int, optional
            Index of the time point (Default value = None)
        position : int, optional
            Index of the XY position (Default value = None)
        row : int, optional
            Index of tile row for XY tiled datasets (Default value = None)
        column : int, optional
            Index of tile column for XY tiled datasets (Default value = None)
        **kwargs :
            Names and int or str positions of any other axes

        Returns
        -------
        image : numpy array
            Image as a 2D numpy array

        """
        pass

    @abstractmethod
    def read_metadata(self, channel: Union[int, str] = None, z: int = None, time: int = None,
                      position: int = None, row: int = None, column: int = None,
                      **kwargs: Union[int, str]) -> Dict[str, Any]:
        """
        Read metadata only.

        Parameters
        ----------
        channel : int or str, optional
            Integer index or string name of the channel (Default value = None)
        z : int, optional
            Index of z slice (Default value = None)
        time : int, optional
            Index of the time point (Default value = None)
        position : int, optional
            Index of the XY position (Default value = None)
        row : int, optional
            Index of tile row for XY tiled datasets (Default value = None)
        column : int, optional
            Index of tile col for XY tiled datasets (Default value = None)
        **kwargs :
            Names and integer positions of any other axes

        Returns
        -------
        metadata : dict
            Image metadata

        """
        pass

    @abstractmethod
    def close(self):
        """
        Close the dataset, releasing any resources it holds
        """
        pass

    @abstractmethod
    def get_image_coordinates_list(self) -> List[Dict[str, Union[int, str]]]:
        """
        Return a list of the coordinates (e.g. {'channel': 'DAPI', 'z': 0, 'time': 0}) of every image in the dataset

        Returns
        -------
        list
            List of image coordinates
        """
        pass

    def await_new_image(self, timeout=None):
        """
        Wait for a new image to arrive in the dataset

        Parameters
        ----------
        timeout : float, optional
            Maximum time to wait in seconds (Default value = None)

        Returns
        -------
        bool
            True if a new image has arrived, False if the timeout was reached
        """
        success = self._new_image_event.wait(timeout=timeout)
        if success:
            self._new_image_event.clear()
        return success

    @abstractmethod
    def is_finished(self) -> bool:
        """
        Check if the dataset is finished and no more images will be added
        """
        pass


    ####### Implementated methods #######
    def as_array(self, axes: List[str] = None, stitched: bool = False,
                 **kwargs: Union[int, str]) -> 'dask.array':
        """
        Create one big Dask array with last two axes as y, x and preceding axes depending on data.
        If the dataset is saved to disk, the dask array is made up of memory-mapped numpy arrays,
        so the dataset does not need to be able to fit into RAM.
        If the data doesn't fully fill out the array (e.g. not every z-slice collected at every time point),
        zeros will be added automatically.

        To convert data into a numpy array, call np.asarray() on the returned result. However, doing so will bring the
        data into RAM, so it may be better to do this on only a slice of the array at a time.

        Parameters
        ----------
        axes : list, optional
            List of axes names over which to iterate and merge into a stacked array.
            If None, all axes will be used in PTCZYX order (Default value = None).
        stitched : bool, optional
            If True and tiles were acquired in a grid, lay out adjacent tiles next to one another
            (Default value = False)
        **kwargs :
            Names and integer positions of axes on which to slice data

        Returns
        -------
        dataset : dask array
            Dask array representing the dataset
        """
        if stitched:
            if self._overlap is None:
                raise Exception('This is not a stitchable dataset')
            if self._full_resolution is None:
                raise Exception('Undefinied whether this is a full resolution dataset or not')

        if None in [self.image_height, self.image_width, self.dtype, self.bytes_per_pixel]:
            raise Exception('Dataset is missing required information to create a dask array')

        if not stitched or not self._full_resolution:
            w = self.image_width
            h = self.image_height
        else:
            w = self.image_width - self._overlap[1]
            h = self.image_height - self._overlap[0]

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
            if _ROW_AXIS in axes_to_stack:
                del axes_to_stack[_ROW_AXIS]
            if _COLUMN_AXIS in axes_to_stack:
                del axes_to_stack[_COLUMN_AXIS]
            if _ROW_AXIS in axes_to_slice:
                del axes_to_slice[_ROW_AXIS]
            if _COLUMN_AXIS in axes_to_slice:
                del axes_to_slice[_COLUMN_AXIS]

        chunks = tuple([(1,) * len(axes_to_stack[axis]) for axis in axes_to_stack.keys()])
        if stitched:
            row_values = np.array(list(self.axes[_ROW_AXIS]))
            column_values = np.array(list(self.axes[_COLUMN_AXIS]))
            chunks += (h * (np.max(row_values) - np.min(row_values) + 1),
                       w * (np.max(column_values) - np.min(column_values) + 1))
        else:
            chunks += (h, w)
        if rgb:
            chunks += (3,)

        array = da.map_blocks(
            partial(self._read_one_image_for_large_array, overlap=self._overlap, full_resolution=self._full_resolution,
                    axes_to_stack=axes_to_stack, axes_to_slice=axes_to_slice, stitched=stitched, rgb=rgb),
            dtype=self.dtype,
            chunks=chunks,
            meta=self._empty_tile
        )

        return array

    def _read_one_image_for_large_array(self, block_id, overlap, full_resolution,
                                        axes_to_stack=None, axes_to_slice=None, stitched=False, rgb=False):
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
                        if np.any(overlap[0] > 0) and full_resolution:
                            min_index = np.floor(overlap / 2).astype(np.int_)
                            max_index = np.ceil(overlap / 2).astype(np.int_)
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

    ####### Private methods #######

    def _infer_image_properties(self, image):
        if self.dtype is None:
            # infer global dtype for as_array method
            self.image_height, self.image_width = image.shape[:2]
            self.dtype = image.dtype
            if self.dtype == np.uint8 and image.ndim == 3:
                self.bytes_per_pixel = 3 # RGB
            else:
                self.bytes_per_pixel = 2 if self.dtype == np.uint16 else 1

    def _update_axes(self, image_coordinates):
        """
        A new image has been added to the dataset, update the axes values and types
        """
        # update and ensure that all axes are either string or integer
        for axis_name, position in image_coordinates.items():
            if axis_name not in self.axes_types:
                if isinstance(position, int):
                    self.axes_types[axis_name] = int
                elif isinstance(position, str):
                    self.axes_types[axis_name] = str
                else:
                    raise RuntimeError("Axis values must be either integers or strings")
            elif type(position) != self.axes_types[axis_name]:
                raise RuntimeError("can't mix String and Integer values along an axis")

        # update the axes that have been seen
        for axis_name in image_coordinates.keys():
            if axis_name not in self.axes.keys():
                self.axes[axis_name] = SortedSet()
                self.axes_types[axis_name] = type(image_coordinates[axis_name])
            self.axes[axis_name].add(image_coordinates[axis_name])

        self._parse_string_axes_values(image_coordinates)

    def _parse_string_axes_values(self, image_coordinates):
        """
        Parse the string axes values to determine the possible values for each string axis,

        Parameters
        ----------
        image_coordinates : dict or Iterable
            A dictionary of axis names and their positions, e.g. {'channel': 0, 'z': 1, 'time': 2} or
            an iterable of frozensets of image_coordinates
        """
        # if this is a new axis_value for a string axis, add a list to populate
        for string_axis_name in [axis_name for axis_name in self.axes_types.keys() if
                                 self.axes_types[axis_name] is str]:
            if string_axis_name not in self._string_axes_values.keys():
                self._string_axes_values[string_axis_name] = []

        # if its called on just one image, make it a list of one image
        if isinstance(image_coordinates, dict):
            image_coordinates = [image_coordinates.items()]

        for single_image_coordinates in image_coordinates:
            for axis_name, axis_value in single_image_coordinates:
                if axis_name in self._string_axes_values.keys() and \
                        axis_value not in self._string_axes_values[axis_name]:
                    self._string_axes_values[axis_name].append(axis_value)


    def _parse_image_keys(self, image_keys):
        """
        Parse the image keys to determine the axes names, types, and possible values
        """
        for image_coordinates in image_keys:
            for axis_name, position in image_coordinates:
                if axis_name not in self.axes.keys():
                    self.axes[axis_name] = SortedSet()
                    self.axes_types[axis_name] = type(position)
                self.axes[axis_name].add(position)
        # Sort axes according to _AXIS_ORDER
        self.axes = dict(sorted(self.axes.items(), key=_get_axis_order_key, reverse=True))

    def _consolidate_axes(self, channel: int or str, z: int, position: int,
                          time: int, row: int, column: int, **kwargs):
        """
        Combine all the axes with standard names and custom names into a single dictionary, eliminating
        any None values. Also, convert any string-valued axes passed as ints into strings
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
