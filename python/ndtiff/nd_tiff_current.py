"""
Library for reading NDTiff datasets
"""
import os
import mmap
import numpy as np
import sys
import json
import platform
import dask.array as da
import warnings
import struct
import threading


class _SingleNDTiffReader:
    """
    Class corresponsing to a single multipage tiff file
    Pass the full path of the TIFF to instantiate and call close() when finished
    """

    # file format constants
    SUMMARY_MD_HEADER = 2355492
    EIGHT_BIT = 0
    SIXTEEN_BIT = 1
    EIGHT_BIT_RGB = 2
    UNCOMPRESSED = 0

    def __init__(self, tiff_path):
        self.tiff_path = tiff_path
        self.file = open(tiff_path, "rb")
        if platform.system() == "Windows":
            self.mmap_file = mmap.mmap(self.file.fileno(), 0, access=mmap.ACCESS_READ)
        else:
            self.mmap_file = mmap.mmap(self.file.fileno(), 0, prot=mmap.PROT_READ)
        self.summary_md, self.first_ifd_offset = self._read_header()
        self.mmap_file.close()

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
        # int.from_bytes(self.mmap_file[24:28], sys.byteorder) # should be equal to 483729 starting in version 1
        self._major_version = int.from_bytes(self._read(12, 16), sys.byteorder)
        self._minor_version = int.from_bytes(self._read(16, 20), sys.byteorder)

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
        # return self.np_memmap[int(start) : int(end)].tobytes()

    def read_metadata(self, index):
        return json.loads(
            self._read(
                index["metadata_offset"], index["metadata_offset"] + index["metadata_length"]
            )
        )

    def read_image(self, index, memmapped=False):
        if index["pixel_type"] == self.EIGHT_BIT_RGB:
            bytes_per_pixel = 3
            dtype = np.uint8
        elif index["pixel_type"] == self.EIGHT_BIT:
            bytes_per_pixel = 1
            dtype = np.uint8
        elif index["pixel_type"] == self.SIXTEEN_BIT:
            bytes_per_pixel = 2
            dtype = np.uint16
        else:
            raise Exception("unrecognized pixel type")
        width = index["image_width"]
        height = index["image_height"]

        if memmapped:
            np_memmap = np.memmap(self.file, dtype=np.uint8, mode="r")
            image = np.reshape(
                np_memmap[
                    index["pixel_offset"] : index["pixel_offset"] + width * height * bytes_per_pixel
                ].view(dtype),
                [height, width, 3] if bytes_per_pixel == 3 else [height, width],
            )
        else:
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

    def __init__(self, dataset_path=None, remote_storage_monitor=None, **kwargs):
        """
        Provides access to an NDTiffStorage dataset,
        either one currently being acquired or one on disk

        Parameters
        ----------
        dataset_path : str
            Abosolute path of top level folder of a dataset on disk
        remote_storage_monitor : JavaObjectShadow
            Object that allows callbacks from remote NDTiffStorage. User's need not call this directly
        """
        self._tile_width = None
        self._tile_height = None
        self._lock = threading.RLock()
        if remote_storage_monitor is not None:
            # this dataset is a view of an active acquisition. The storage exists on the java side
            self._new_image_arrived = False # used by custom (e.g. napari) viewer to check for updates. Will be reset to false by them
            self._remote_storage_monitor = remote_storage_monitor
            self.summary_metadata = self._remote_storage_monitor.get_summary_metadata()
            if "GridPixelOverlapX" in self.summary_metadata.keys():
                self._tile_width = (
                    self.summary_metadata["Width"] - self.summary_metadata["GridPixelOverlapX"]
                )
                self._tile_height = (
                    self.summary_metadata["Height"] - self.summary_metadata["GridPixelOverlapY"]
                )
            if dataset_path is None:
                self.path = remote_storage_monitor.get_disk_location()
            else:
                self.path = dataset_path #Overriden by pyramid storage class creating this
            self.path += "" if self.path[-1] == os.sep else os.sep
            self.axes = {}
            self.index = {}
            self._readers_by_filename = {}
            return

        self._remote_storage_monitor = None
        self.path = dataset_path
        self.path += "" if self.path[-1] == os.sep else os.sep
        self.index = self.read_index(self.path)
        tiff_names = [
            os.path.join(self.path, tiff) for tiff in os.listdir(self.path) if tiff.endswith(".tif")
        ]
        self._readers_by_filename = {}
        # Count how many files need to be opened
        num_tiffs = 0
        count = 0
        for file in os.listdir(self.path):
            if file.endswith(".tif"):
                num_tiffs += 1
        # populate list of readers and tree mapping indices to readers
        for tiff in tiff_names:
            print("\rOpening file {} of {}...".format(count + 1, num_tiffs), end="")
            count += 1
            self._readers_by_filename[tiff.split(os.sep)[-1]] = _SingleNDTiffReader(tiff)
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
        for axes_combo in self.index.keys():
            for axis, position in axes_combo:
                if axis not in self.axes.keys():
                    self.axes[axis] = set()
                self.axes[axis].add(position)

        # figure out the mapping of channel name to position by reading image metadata
        print("\rReading channel names...", end="")
        self._read_channel_names()
        print("\rFinished reading channel names", end="")


        # get information about image width and height, assuming that they are consistent for whole dataset
        # (which is not neccessarily true but convenient when it is)
        with self._lock:
            first_index = list(self.index.values())[0]
        self._parse_first_index(first_index)

        print("\rDataset opened                ")

    def get_channel_names(self):
        return self.channel_names.keys()

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
        channel=0,
        z=None,
        time=None,
        position=None,
        channel_name=None,
        row=None,
        col=None,
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
        channel_name : str
            Name of the channel. Overrides channel index if supplied (Default value = None)
        row : int
            index of tile row for XY tiled datasets (Default value = None)
        col : int
            index of tile col for XY tiled datasets (Default value = None)
        **kwargs :
            names and integer positions of any other axes

        Returns
        -------
        bool :
            indicating whether the dataset has an image matching the specifications
        """
        with self._lock:
            return self._does_have_image(
                _consolidate_axes(self.get_channel_names(), channel, channel_name, z, position, time, row, col, kwargs)
            )

    def read_image(
        self,
        channel=0,
        z=None,
        time=None,
        position=None,
        row=None,
        col=None,
        channel_name=None,
        memmapped=False,
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
        channel_name :
            Name of the channel. Overrides channel index if supplied (Default value = None)
        row : int
            index of tile row for XY tiled datasets (Default value = None)
        col : int
            index of tile col for XY tiled datasets (Default value = None)
        memmapped : bool
             (Default value = False)
        **kwargs :
            names and integer positions of any other axes

        Returns
        -------
        image : numpy array or tuple
            image as a 2D numpy array, or tuple with image and image metadata as dict

        """
        with self._lock:
            axes = _consolidate_axes(self.get_channel_names(),
                channel, channel_name, z, position, time, row, col, kwargs
            )

            return self._do_read_image(axes, memmapped)

    def read_metadata(
        self,
        channel=0,
        z=None,
        time=None,
        position=None,
        channel_name=None,
        row=None,
        col=None,
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
        channel_name :
            Name of the channel. Overrides channel index if supplied (Default value = None)
        row : int
            index of tile row for XY tiled datasets (Default value = None)
        col : int
            index of tile col for XY tiled datasets (Default value = None)

        **kwargs :
            names and integer positions of any other axes

        Returns
        -------
        metadata : dict

        """
        with self._lock:
            axes = _consolidate_axes(self.get_channel_names(),
                channel, channel_name, z, position, time, row, col, kwargs
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
        """
        with self._lock:
            _, axes, index_entry = self._read_single_index_entry(data, self.index)

            if index_entry["filename"] not in self._readers_by_filename:
                self._readers_by_filename[index_entry["filename"]] = _SingleNDTiffReader(
                    self.path + index_entry["filename"]
                )

            # update the axes that have been seen
            for axis_name in axes.keys():
                if axis_name not in self.axes.keys():
                    self.axes[axis_name] = set()
                self.axes[axis_name].add(axes[axis_name])

            # update the map of channel names to channel indices
            self._read_channel_names()

        if not hasattr(self, 'image_width'):
            self._parse_first_index(index_entry)

        return axes

    def _read_channel_names(self):
        if 'ChNames' in self.summary_metadata:
            # It was created by a MM MDA/Clojure acquistiion engine
            self.channel_names = {name: i for i, name in enumerate(self.summary_metadata['ChNames'])}
        else:
            # AcqEngJ
            if _CHANNEL_AXIS in self.axes.keys():
                self.channel_names = {}
                for key in self.index.keys():
                    axes = {axis: position for axis, position in key}
                    if (
                        _CHANNEL_AXIS in axes.keys()
                        and axes[_CHANNEL_AXIS] not in self.channel_names.values()
                    ):
                        channel_name = self.read_metadata(**axes)["Channel"]
                        self.channel_names[channel_name] = axes[_CHANNEL_AXIS]
                    if len(self.channel_names.values()) == len(self.axes[_CHANNEL_AXIS]):
                        break


    def _parse_first_index(self, first_index):
        """
        Read through first index to get some global data about images (assuming it is same for all)
        """
        if first_index["pixel_type"] == _SingleNDTiffReader.EIGHT_BIT_RGB:
            self.bytes_per_pixel = 3
            self.dtype = np.uint8
        elif first_index["pixel_type"] == _SingleNDTiffReader.EIGHT_BIT:
            self.bytes_per_pixel = 1
            self.dtype = np.uint8
        elif first_index["pixel_type"] == _SingleNDTiffReader.SIXTEEN_BIT:
            self.bytes_per_pixel = 2
            self.dtype = np.uint16

        self.image_width = first_index["image_width"]
        self.image_height = first_index["image_height"]
        if "GridPixelOverlapX" in self.summary_metadata:
            self._tile_width = self.image_width - self.summary_metadata["GridPixelOverlapX"]
            self._tile_height = self.image_height - self.summary_metadata["GridPixelOverlapY"]


    def _add_storage_monitor_fn(self, acquisition, storage_monitor_fn, callback_fn=None, debug=False):
        """
        Add a callback function that gets called whenever a new image is writtern to disk (for acquisitions in
        progress only)

        Parameters
        ----------
        callback_fn : Callable
            callable with that takes 1 argument, the axes dict of the image just written
        """
        if self._remote_storage_monitor is None:
            raise Exception("Only valid for datasets with writing in progress")

        connected_event = threading.Event()

        push_port = self._remote_storage_monitor.get_port()
        monitor_thread = threading.Thread(
            target=storage_monitor_fn,
            args=(
                acquisition,
                self,
                push_port,
                connected_event,
                callback_fn,
                debug,
            ),
            name="ImageSavedCallbackThread",
        )

        monitor_thread.start()

        # Wait for pulling to start before you signal for pushing to start
        connected_event.wait()  # wait for push/pull sockets to connect

        # start pushing out all the image written events (including ones that have already accumulated)
        self._remote_storage_monitor.start()
        return monitor_thread

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
        with open(path + os.sep + "NDTiff.index", "rb") as index_file:
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
        memmapped=False,
    ):
        # determine which reader contains the image
        key = frozenset(axes.items())
        if key not in self.index:
            raise Exception("image with keys {} not present in data set".format(key))
        index = self.index[key]
        reader = self._readers_by_filename[index["filename"]]
        return reader.read_image(index, memmapped)

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
            list of axes names over which to iterate and merge into a stacked array. If None, all axes will be used.
            The order of axes supplied in this list will be the order of the axes of the returned dask array
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
        w = self.image_width if not stitched else self._tile_width
        h = self.image_height if not stitched else self._tile_height
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

        def read_one_image(block_id, axes_to_stack=axes_to_stack, axes_to_slice=axes_to_slice):
            # a function that reads in one chunk of data
            axes = {key: block_id[i] for i, key in enumerate(axes_to_stack.keys())}
            if stitched:
                # Combine all rows and cols into one stitched image
                self.half_overlap = (self.overlap[0] // 2, self.overlap[1] // 2)
                # get spatial layout of position indices
                row_values = np.array(list(self.axes["row"]))
                column_values = np.array(list(self.axes["column"]))
                # make nested list of rows and columns
                blocks = []
                for row in row_values:
                    blocks.append([])
                    for column in column_values:
                        # remove overlap between tiles
                        if not self.has_image(**axes, **axes_to_slice, row=row, column=column):
                            blocks[-1].append(self._empty_tile)
                        else:
                            tile = self.read_image(**axes, **axes_to_slice, row=row, column=column, memmapped=True)
                            if self.half_overlap[0] != 0:
                                tile = tile[
                                       self.half_overlap[0]: -self.half_overlap[0],
                                       self.half_overlap[1]: -self.half_overlap[1],
                                       ]
                            blocks[-1].append(tile)

                if rgb:
                    image = np.concatenate(
                        [
                            np.concatenate(row, axis=len(blocks[0][0].shape) - 2)
                            for row in blocks
                        ],
                        axis=0,
                    )
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

        chunks = tuple([(1,) * len(axes_to_stack[axis]) for axis in axes_to_stack.keys()])
        chunks += (w, h)
        if rgb:
            chunks += (3,)

        array = da.map_blocks(
            read_one_image,
            dtype=self.dtype,
            chunks=chunks,
            meta=self._empty_tile
        )

        return array


class NDTiffPyramidDataset():
    """Class that opens a single NDTiffStorage multi-resolution pyramid dataset"""

    def __init__(self, dataset_path=None, full_res_only=True, remote_storage_monitor=None):
        """
        Provides access to a NDTiffStorage pyramid dataset,
        either one currently being acquired or one on disk

        Parameters
        ----------
        dataset_path : str
            Abosolute path of top level folder of a dataset on disk
        full_res_only : bool
            Only open the full resolution data to save time
        remote_storage_monitor : JavaObjectShadow
            Object that allows callbacks from remote NDTiffStorage. Users need not call this directly
        """
        self._lock = threading.RLock()
        if remote_storage_monitor is not None:
            self._remote_storage_monitor = None # IT belongs to the full resolution subclass
            # this dataset is a view of an active acquisition. The storage exists on the java side
            self.path = remote_storage_monitor.get_disk_location()
            self.path += "" if self.path[-1] == os.sep else os.sep

            with self._lock:
                self.res_levels = {
                    0: NDTiffDataset(remote_storage_monitor=remote_storage_monitor,
                                     dataset_path=self.path + "Full resolution" + os.sep)
                }
            # No information related higher res levels when remote storage monitoring right now

            #Copy stuff from the full res class for convenience
            self.summary_metadata = self.res_levels[0].summary_metadata
            self.axes = self.res_levels[0].axes

            return

        # Loading from disk
        self._remote_storage_monitor = None
        self.path = dataset_path
        self.path += "" if self.path[-1] == os.sep else os.sep
        res_dirs = [
            dI for dI in os.listdir(dataset_path) if os.path.isdir(os.path.join(dataset_path, dI))
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
            if full_res_only and res_dir != "Full resolution":
                continue
            res_dir_path = os.path.join(dataset_path, res_dir)
            res_level = NDTiffDataset(dataset_path=res_dir_path)
            if res_dir == "Full resolution":
                with self._lock:
                    self.res_levels[0] = res_level
                # get summary metadata and index tree from full resolution image
                self.summary_metadata = res_level.summary_metadata

                self.overlap = (
                    np.array([
                            self.summary_metadata["GridPixelOverlapY"],
                            self.summary_metadata["GridPixelOverlapX"],
                        ])
                )

                self.axes = res_level.axes
                self._channel_names = res_level.channel_names
                self.bytes_per_pixel = res_level.bytes_per_pixel
                self.dtype = res_level.dtype
                self.image_width = res_level.image_width
                self.image_height = res_level.image_height

            else:
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
            list of axes names over which to iterate and merge into a stacked array. If None, all axes will be used.
            The order of axes supplied in this list will be the order of the axes of the returned dask array
        stitched : bool
            Lay out adjacent tiles next to one another to form a larger image (Default value = False)
        **kwargs :
            names and integer positions of axes on which to slice data
        Returns
        -------
        dataset : dask array
        """
        return self.res_levels[0].as_array(axes=axes, stitched=stitched, **kwargs)

    def has_image(
        self,
        channel=0,
        z=None,
        time=None,
        position=None,
        channel_name=None,
        resolution_level=0,
        row=None,
        col=None,
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
        channel_name : str
            Name of the channel. Overrides channel index if supplied (Default value = None)
        row : int
            index of tile row for XY tiled datasets (Default value = None)
        col : int
            index of tile col for XY tiled datasets (Default value = None)
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
                channel_name=channel_name,
                row=row,
                col=col,
                **kwargs
            )

    def read_image(
        self,
        channel=0,
        z=None,
        time=None,
        position=None,
        row=None,
        col=None,
        channel_name=None,
        resolution_level=0,
        memmapped=False,
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
        channel_name :
            Name of the channel. Overrides channel index if supplied (Default value = None)
        row : int
            index of tile row for XY tiled datasets (Default value = None)
        col : int
            index of tile col for XY tiled datasets (Default value = None)
        resolution_level :
            0 is full resolution, otherwise represents downampling of pixels
            at 2 ** (resolution_level) (Default value = 0)
        memmapped : bool
             (Default value = False)
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
                    col=col,
                    channel_name=channel_name,
                    memmapped=memmapped,
                    **kwargs)

    def read_metadata(
        self,
        channel=0,
        z=None,
        time=None,
        position=None,
        channel_name=None,
        row=None,
        col=None,
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
        channel_name :
            Name of the channel. Overrides channel index if supplied (Default value = None)
        row : int
            index of tile row for XY tiled datasets (Default value = None)
        col : int
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
                    channel=0,
                    z=z,
                    time=time,
                    position=position,
                    channel_name=channel_name,
                    row=row,
                    col=col,
                    **kwargs)

    # TODO: this needs to be cleaned up probably--seems like this functionality belongs in pycromanager
    def _add_storage_monitor_fn(self, acquisition, storage_monitor_fn, callback_fn=None, debug=False):
        # Only valid for the full res data
        return self.res_levels[0]._add_storage_monitor_fn(acquisition, storage_monitor_fn, callback_fn, debug)

    def _add_index_entry(self, index_entry):
        # Pass through to full res data
        return self.res_levels[0]._add_index_entry(index_entry)

    def close(self):
        with self._lock:
            for res_level in self.res_levels:
                res_level.close()


_POSITION_AXIS = "position"
_ROW_AXIS = "row"
_COLUMN_AXIS = "column"
_Z_AXIS = "z"
_TIME_AXIS = "time"
_CHANNEL_AXIS = "channel"

def _consolidate_axes(channel_names, channel, channel_name, z, position, time, row, col, kwargs):
    """
    Get all axis names in a consistent format
    """
    axes = {}
    if channel is not None:
        axes[_CHANNEL_AXIS] = channel
    if channel_name is not None:
        axes[_CHANNEL_AXIS] = channel_names[channel_name]
    if z is not None:
        axes[_Z_AXIS] = z
    if position is not None:
        axes[_POSITION_AXIS] = position
    if time is not None:
        axes[_TIME_AXIS] = time
    if row is not None:
        axes[_ROW_AXIS] = row
    if col is not None:
        axes[_COLUMN_AXIS] = col
    for other_axis_name in kwargs.keys():
        axes[other_axis_name] = kwargs[other_axis_name]
    return axes
