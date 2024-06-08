import os
import threading
import numpy as np
import warnings
import dask.array as da
from sortedcontainers import SortedSet
from functools import partial
import re

from ndtiff.file_io import NDTiffFileIO, BUILTIN_FILE_IO
from ndtiff.ndtiff_file import SingleNDTiffReader
from ndtiff.ndtiff_file import _CHANNEL_AXIS
from ndtiff.ndtiff_index import NDTiffIndexEntry, read_ndtiff_index

from ndtiff.ndtiff_file import SingleNDTiffWriter

from ndtiff.ndstorage_api import WritableNDStorage

class NDTiffDataset(WritableNDStorage):
    """
    Class that opens a single NDTiff dataset
    """

    def __init__(self, dataset_path=None, file_io: NDTiffFileIO = BUILTIN_FILE_IO, summary_metadata=None,
                 name=None, writable=False, **kwargs):
        """
        Provides access to an NDTiffStorage dataset,
        either one currently being acquired or one on disk

        Parameters
        ----------
        dataset_path : str
            Abosolute path of top level folder of a dataset on disk
        file_io: ndtiff.file_io.NDTiffFileIO
            A container containing various methods for interacting with files.
        summary_metadata : dict
            Summary metadata for a dataset that is currently being written by another process
        name : str
            Name of the dataset if writing a new dataset
        writable : bool
            Whether it is a new dataset being written to disk
        """

        self.file_io = file_io
        self._lock = threading.RLock()
        if summary_metadata is not None or writable:
            # this dataset is either:
            #   - a view of an active acquisition. Image data is being written by code on the Java side
            #   - a new dataset being written to disk
            self._new_image_arrived = False # used by custom (e.g. napari) viewer to check for updates. Will be reset to false by them
            self.index = {}
            self._readers_by_filename = {}
            self._summary_metadata = summary_metadata
            self._writable = writable
            self.current_writer = None
            self.file_index = 0
            self.name = name
            self._write_pending_images = {}
            self._finished_event = threading.Event()

            if writable and name is not None:
                # create a folder to hold the new Tiff files
                self.path = _create_unique_acq_dir(dataset_path, name)
            else:
                self.path = dataset_path
                self.path += "" if self.path[-1] == os.sep else os.sep
        else:
            self._write_pending_images = None
            self._writable = False
            self.path = dataset_path
            self.path += "" if self.path[-1] == os.sep else os.sep
            print("\rReading index...          ", end="")
            with self.file_io.open(os.sep.join((self.path, "NDTiff.index")), "rb") as index_file:
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
                new_reader = SingleNDTiffReader(tiff, file_io=self.file_io)
                self._readers_by_filename[tiff.split(os.sep)[-1]] = new_reader
                # Should be the same on every file so resetting them is fine
                self.major_version, self.minor_version = new_reader.major_version, new_reader.minor_version

            if len(self._readers_by_filename) > 0:
                self.summary_metadata = list(self._readers_by_filename.values())[0].summary_md

            # TODO: make overlap non-public in a future version
            self.overlap = (
                np.array([self.summary_metadata["GridPixelOverlapY"], self.summary_metadata["GridPixelOverlapX"]])
                if "GridPixelOverlapY" in self.summary_metadata else None)
            self._overlap = self.overlap

            self._parse_image_keys(self.index.keys())

            # figure out the mapping of channel name to position by reading image metadata
            self._channels = {}
            self._update_channel_names(self.index.keys())

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
        # TODO: I think this check is no longer needed. comment out for now and remove later
        # if not hasattr(self, '_new_image_arrived'):
        #     return False # pre-initilization
        new = self._new_image_arrived
        self._new_image_arrived = False
        return new

    def has_image(self, channel=None, z=None, time=None, position=None, row=None, column=None, **kwargs):
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
            axes = self._consolidate_axes(channel, z, position, time, row, column, **kwargs)

            if self._write_pending_images is not None and len(self._write_pending_images) > 0:
                if frozenset(axes.items()) in self._write_pending_images:
                    return True

            return self._does_have_image(axes)

    def read_image(self, channel=None, z=None, time=None, position=None, row=None, column=None, **kwargs):
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
            axes = self._consolidate_axes(channel, z, position, time, row, column, **kwargs)

            if self._write_pending_images is not None and len(self._write_pending_images) > 0:
                if frozenset(axes.items()) in self._write_pending_images:
                    return self._write_pending_images[frozenset(axes.items())][0]


            return self._do_read_image(axes)

    def read_metadata(self, channel=None, z=None, time=None, position=None, row=None, column=None, **kwargs):
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
            axes = self._consolidate_axes(channel, z, position, time, row, column, **kwargs)

            if self._write_pending_images is not None and len(self._write_pending_images) > 0:
                if frozenset(axes.items()) in self._write_pending_images:
                    return self._write_pending_images[frozenset(axes.items())][1]

            return self._do_read_metadata(axes)

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
        if not self._writable:
            raise RuntimeError("Cannot write to a read-only dataset")

        self._update_axes_types(coordinates)

        # add to write pending images
        self._write_pending_images[frozenset(coordinates.items())] = (image, metadata)

        # write the image to disk
        if self.current_writer is None:
            filename = 'NDTiffStack.tif'
            if self.name is not None:
                filename = self.name + '_' + filename
            self.current_writer = SingleNDTiffWriter(self.path, filename, self._summary_metadata)
            self.file_index += 1
            # create the index file
            self._index_file = open(os.path.join(self.path, "NDTiff.index"), "wb")
        elif not self.current_writer.has_space_to_write(image, metadata):
            self.current_writer.finished_writing()
            filename = 'NDTiffStack_{}.tif'.format(self.file_index)
            if self.name is not None:
                filename = self.name + '_' + filename
            self.current_writer = SingleNDTiffWriter(self.path, filename, self._summary_metadata)
            self.file_index += 1


        index_data_entry = self.current_writer.write_image(frozenset(coordinates.items()), image, metadata)
        # create readers and update axes
        self.add_index_entry(index_data_entry)
        # write the index to disk
        self._index_file.write(index_data_entry.as_byte_buffer().getvalue())
        # remove from pending images
        del self._write_pending_images[frozenset(coordinates.items())]


    def finish(self):
        if self.current_writer is not None:
            self.current_writer.finished_writing()
            self.current_writer = None
        self._index_file.close()
        self._finished_event.set()

    def is_finished(self) -> bool:
        return self._finished_event.is_set()

    def initialize(self, summary_metadata: dict):
        # called for new storage
        self._summary_metadata = summary_metadata

    def block_until_finished(self, timeout=None):
        self._finished_event.wait(timeout=timeout)

    def get_image_coordinates_list(self):
        frozen_set_list = list(self.index.keys())
        # convert to dict
        return [{axis_name: position for axis_name, position in key} for key in frozen_set_list]

    # TODO: remove this in a future version
    def get_index_keys(self):
        """
        Return a list of every combination of axes that has a image in this dataset
        """
        warnings.warn("get_index_keys is deprecated, use get_image_coordinates_list instead", DeprecationWarning)
        return self.get_image_coordinates_list()

    def add_index_entry(self, data):
        """
        Add entry for an image that has been received and is now on disk
        This is used when the data is being written outside this class (i.e. by Java code)

        Parameters
        ----------
        data : bytes or NDTiffIndexEntry
            binary data for the index entry
        """
        with self._lock:
            if isinstance(data, NDTiffIndexEntry):
                index_entry = data
                # reconvert to dict from frozenset
                image_coordinates = {axis_name: position for axis_name, position in index_entry.axes_key}
                self.index[index_entry.axes_key] = index_entry
            else:
                _, image_coordinates, index_entry = NDTiffIndexEntry.unpack_single_index_entry(data)
                self.index[frozenset(image_coordinates.items())] = index_entry

            if index_entry.filename not in self._readers_by_filename:
                new_reader = SingleNDTiffReader(os.path.join(self.path, index_entry.filename), file_io=self.file_io)
                self._readers_by_filename[index_entry.filename] = new_reader
                # Should be the same on every file so resetting them is fine
                self.major_version, self.minor_version = new_reader.major_version, new_reader.minor_version

            self._new_image_available(image_coordinates)

            # update the map of channel names to channel indices
            self._update_channel_names(image_coordinates)

        if not hasattr(self, 'image_width'):
            self._parse_first_index(index_entry)

        return image_coordinates

    def _update_channel_names(self, image_coordinates):
        """
        As of NDTiff 3.2, axes are allowed to take string values: e.g. {'channel': 'DAPI'}
        This is allowed on any axis. This function returns a tuple of possible values along
        the string axis in order to be able to interconvert integer values and string values.

        param single_image_coordinates: if provided, only parse this newly added entry
        """
        # iterate through the key_combos for each image
        if self.major_version >= 3 and self.minor_version >= 2:

            self._parse_string_axes_values(image_coordinates)

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
                        single_image_coordinates = {axis: position for axis, position in key}
                        if (_CHANNEL_AXIS in single_image_coordinates.keys()
                                    and single_image_coordinates[_CHANNEL_AXIS] not in self._channels.values()):
                            channel_name = self.read_metadata(**single_image_coordinates)["Channel"]
                            self._channels[channel_name] = single_image_coordinates[_CHANNEL_AXIS]
                        if len(self._channels.values()) == len(self.axes[_CHANNEL_AXIS]):
                            break

    def _parse_first_index(self, first_index):
        """
        Read through first index to get some global data about images (assuming it is same for all)
        """
        if first_index["pixel_type"] == SingleNDTiffReader.EIGHT_BIT_RGB:
            self.bytes_per_pixel = 3
            self.dtype = np.uint8
        elif first_index["pixel_type"] == SingleNDTiffReader.EIGHT_BIT_MONOCHROME:
            self.bytes_per_pixel = 1
            self.dtype = np.uint8
        elif first_index["pixel_type"] == SingleNDTiffReader.SIXTEEN_BIT_MONOCHROME or \
                first_index["pixel_type"] == SingleNDTiffReader.FOURTEEN_BIT_MONOCHROME or \
                first_index["pixel_type"] == SingleNDTiffReader.TWELVE_BIT_MONOCHROME or \
                first_index["pixel_type"] == SingleNDTiffReader.TEN_BIT_MONOCHROME or \
                first_index["pixel_type"] == SingleNDTiffReader.ELEVEN_BIT_MONOCHROME:
            self.bytes_per_pixel = 2
            self.dtype = np.uint16

        self.image_width = first_index["image_width"]
        self.image_height = first_index["image_height"]

    def _does_have_image(self, axes):
        key = frozenset(axes.items())
        return key in self.index

    def _do_read_image(self, axes,):
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



def _create_unique_acq_dir(root, prefix):
    if not os.path.exists(root):
        os.makedirs(root)

    pattern = re.compile(re.escape(prefix) + r"_(\d+).*", re.IGNORECASE)
    max_number = 0

    for acq_dir in os.listdir(root):
        match = pattern.match(acq_dir)
        if match:
            try:
                number = int(match.group(1))
                max_number = max(max_number, number)
            except ValueError:
                pass

    new_dir_name = f"{prefix}_{max_number + 1}"
    new_dir_path = os.path.join(root, new_dir_name)
    os.makedirs(new_dir_path)

    return new_dir_path