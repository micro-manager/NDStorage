import numpy as np
from ndtiff import Dataset, file_io
import os
import pytest
from typing import List

class BadOpenError(Exception):
    pass

class BadListdirError(Exception):
    pass

class BadPathJoinError(Exception):
    pass

class BadIsDirError(Exception):
    pass

def bad_open_function(*args, **kwargs):
    raise BadOpenError

def bad_listdir(*args, **kwargs):
    raise BadListdirError

def bad_path_join(*args, **kwargs):
    raise BadPathJoinError

def bad_isdir(*args, **kwargs):
    raise BadIsDirError

def test_bad_open(test_data_path):
    data_path_list: List[str] = [
        os.path.join(test_data_path, "v2", "ndtiffv2.0_test"),
        os.path.join(test_data_path, 'v3', 'ndtiffv3.0_test'),
        os.path.join(test_data_path, "v2", "ndtiffv2.0_stitched_test"),
        os.path.join(test_data_path, 'v3', 'ndtiffv3.0_stitched_test'),
        os.path.join(test_data_path, 'v3', 'Magellan_expolore_multi_channel'),
        os.path.join(test_data_path, 'v3', 'Magellan_expolore_negative_indices_and_overwritten'),
        os.path.join(test_data_path, 'v3', 'Nonstandard_axis_names'),
        os.path.join(test_data_path, 'v3', 'mm_mda_tcz_15'),
        os.path.join(test_data_path, 'v3', 'ndtiff3.2_multichannel'),
        os.path.join(test_data_path, 'v3', 'ndtiff3.2_monochrome'),
        os.path.join(test_data_path, 'v3', 'ndtiff3.2_magellan_explore_rgb'),
        os.path.join(test_data_path, 'v3', '12_bit_pycromanager_mda'),
    ]
    
    # Ensure each dataset loads using the custom open command by raising an exception within it
    # and checking that this exception occured
    for data_path in data_path_list:
        with pytest.raises(BadOpenError):
            Dataset(data_path, file_io=file_io.NDTiffFileIO(open_function=bad_open_function))
        with pytest.raises(BadListdirError):
            Dataset(data_path, file_io=file_io.NDTiffFileIO(listdir_function=bad_listdir))
        with pytest.raises(BadPathJoinError):
            Dataset(data_path, file_io=file_io.NDTiffFileIO(listdir_function=bad_path_join))
        with pytest.raises(BadIsDirError):
            Dataset(data_path, file_io=file_io.NDTiffFileIO(listdir_function=bad_isdir))