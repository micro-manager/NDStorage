from typing import List, Dict, Union

import threading
from ndstorage.ndstorage_base import WritableNDStorageAPI, NDStorageBase


class NDRAMDataset(NDStorageBase, WritableNDStorageAPI):
    """
    A class for holding data in RAM
    Implements the methods needed to be a DataSink for AcqEngPy
    """

    def __init__(self):
        super().__init__()
        self.images = {}
        self.image_metadata = {}
        self._finished_event = threading.Event()

    def initialize(self, summary_metadata: dict):
        self.summary_metadata = summary_metadata

    def block_until_finished(self, timeout=None):
        return self._finished_event.wait(timeout=timeout)

    def finish(self):
        self._finished_event.set()

    def is_finished(self) -> bool:
        return self._finished_event.is_set()

    def put_image(self, coordinates, image, metadata):
        key = frozenset(coordinates.items())
        self.images[key] = image
        self.image_metadata[key] = metadata
        self._update_axes(coordinates)
        self._new_image_event.set()

    def get_image_coordinates_list(self) -> List[Dict[str, Union[int, str]]]:
        frozen_set_list = list(self.images.keys())
        # convert to dict
        return [{axis_name: position for axis_name, position in key} for key in frozen_set_list]

    #### ND Storage API ####
    def close(self):
        self.images = {}
        self.image_metadata = {}
        self.axes = {}

    def has_image(self, channel=None, z=None, time=None, position=None, row=None, column=None, **kwargs):
        axes = self._consolidate_axes(channel, z, position, time, row, column, **kwargs)
        key = frozenset(axes.items())
        return key in self.images.keys()

    def read_image(self, channel=None, z=None, time=None, position=None, row=None, column=None, **kwargs):
        axes = self._consolidate_axes(channel, z, position, time, row, column, **kwargs)
        key = frozenset(axes.items())
        if key not in self.images.keys():
            raise Exception("image with keys {} not present in data set".format(key))
        return self.images[key]

    def read_metadata(self, channel=None, z=None, time=None, position=None, row=None, column=None, **kwargs):
        axes = self._consolidate_axes(channel, z, position, time, row, column, **kwargs)
        key = frozenset(axes.items())
        if key not in self.images.keys():
            raise Exception("image with keys {} not present in data set".format(key))
        return self.image_metadata[key]

