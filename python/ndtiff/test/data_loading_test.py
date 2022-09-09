import numpy as np
from ndtiff import Dataset
import os

test_data_path = os.getcwd() + os.sep + 'test_data' + os.sep

def test_v2_data():
    data_path = test_data_path + "v2/ndtiffv2.0_test"
    dataset = Dataset(data_path)
    assert(np.mean(dataset.as_array()) > 0)

def test_v3_data():
    data_path = test_data_path + 'v3/ndtiffv3.0_test'
    dataset = Dataset(data_path)
    assert(np.mean(dataset.as_array()) > 0)

def test_v2_stitched_data():
    data_path = test_data_path + "v2/ndtiffv2.0_stitched_test"
    dataset = Dataset(data_path)
    stitched = np.array(dataset.as_array(stitched=True))
    assert(stitched[..., 0, 0] > 0)
    assert(stitched[..., -1, -1] > 0)
    assert(stitched[..., 0, -1] == 0)
    assert(stitched[..., -1, 0] == 0)

def test_v3_stitched_data():
    data_path = test_data_path + 'v3/ndtiffv3.0_stitched_test'
    dataset = Dataset(data_path)
    stitched = np.array(dataset.as_array(stitched=True))
    assert(stitched[..., 0, 0] > 0)
    assert(stitched[..., -1, -1] > 0)
    assert(stitched[..., 0, -1] == 0)
    assert(stitched[..., -1, 0] == 0)

def test_v3_magellan_explore():
    data_path = test_data_path + 'v3/Magellan_expolore_multi_channel'
    dataset = Dataset(data_path)
    stitched = np.array(dataset.as_array(stitched=True))
    
def test_v3_magellan_explore_negative_and_overwritten():
    data_path = test_data_path + 'v3/Magellan_expolore_negative_indices_and_overwritten'
    dataset = Dataset(data_path)
    stitched = np.array(dataset.as_array(stitched=True))

def test_v3_non_ctzp_axes():
    data_path = test_data_path + 'v3/Nonstandard_axis_names'
    dataset = Dataset(data_path)
    stitched = np.array(dataset.as_array())

def test_v3_mm_mda():
    data_path = test_data_path + 'v3/mm_mda_tcz_15'
    dataset = Dataset(data_path)
    stitched = np.array(dataset.as_array())

