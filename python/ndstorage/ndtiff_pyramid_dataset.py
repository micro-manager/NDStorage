import os
import numpy as np
import threading
from ndstorage.file_io import NDTiffFileIO, BUILTIN_FILE_IO
from ndstorage.ndtiff_dataset import NDTiffDataset

class NDTiffPyramidDataset:
    """Class that opens a single NDTiffStorage multi-resolution pyramid dataset"""

    def __init__(self, dataset_path=None, file_io: NDTiffFileIO = BUILTIN_FILE_IO, summary_metadata=None):
        """
        Provides access to a NDTiffStorage pyramid dataset,
        either one currently being acquired or one on disk

        Parameters
        ----------
        dataset_path : str
            Abosolute path of top level folder of a dataset on disk
        file_io: ndtiff.file_io.NDTiffFileIO
            A container containing various methods for interacting with files.
        summary_metadata : dict
            Summary metadata, only not None for in progress datasets. Users need not call directly
        """
        self.file_io = file_io
        self._lock = threading.RLock()
        if summary_metadata is not None:
            # this dataset is a view of an active acquisition. The storage exists on the java side
            self.path = dataset_path
            self.path += "" if self.path[-1] == os.sep else os.sep
            self.summary_metadata = summary_metadata

            with self._lock:
                full_res = NDTiffDataset(dataset_path=self.path + "Full resolution" + os.sep,
                                         summary_metadata=summary_metadata, file_io=file_io)
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

    # for backwards compatibility in case of older pycromanager version, can be removed in the future
    def _add_index_entry(self, index_entry):
        self.add_index_entry(index_entry)

    def add_index_entry(self, index_entry):
        # Pass through to full res data
        return self.res_levels[0].add_index_entry(index_entry)

    def close(self):
        with self._lock:
            for res_level in self.res_levels:
                res_level.close()
