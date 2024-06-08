from typing import List, Dict, Union

from pycromanager.acquisition.acq_eng_py.main.acq_eng_metadata import AcqEngMetadata
import numpy as np
from sortedcontainers import SortedSet
import threading
from ndtiff.ndstorage_api import WritableNDStorage


class NDRAMStorage(WritableNDStorage):
    """
    A class for holding data in RAM
    Implements the methods needed to be a DataSink for AcqEngPy
    """

    def __init__(self):
        self.images = {}
        self.image_metadata = {}
        self._finished_event = threading.Event()

    def initialize(self, summary_metadata: dict):
        self.summary_metadata = summary_metadata

    def block_until_finished(self, timeout=None):
        self._finished_event.wait(timeout=timeout)

    def finish(self):
        self._finished_event.set()

    def is_finished(self) -> bool:
        return self._finished_event.is_set()

    def put_image(self, coordinates, image, metadata):
        key = frozenset(coordinates.items())
        self.images[key] = image
        self.image_metadata[key] = metadata
        for axis in coordinates.keys():
            if axis not in self.axes:
                self.axes[axis] = SortedSet()
            self.axes[axis].add(coordinates[axis])
        self._new_image_arrived = True

        self._new_image_available(coordinates)

    def get_image_coordinates_list(self) -> List[Dict[str, Union[int, str]]]:
        frozen_set_list = list(self.images.keys())
        # convert to dict
        return [{axis_name: position for axis_name, position in key} for key in frozen_set_list]

    # TODO: what to do with this?
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
        if key not in self.index:
            raise Exception("image with keys {} not present in data set".format(key))
        return self.images[key]

    def read_metadata(self, channel=None, z=None, time=None, position=None, row=None, column=None, **kwargs):
        axes = self._consolidate_axes(channel, z, position, time, row, column, **kwargs)
        key = frozenset(axes.items())
        if key not in self.index:
            raise Exception("image with keys {} not present in data set".format(key))
        return self.image_metadata[key]

