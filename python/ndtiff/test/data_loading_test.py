import numpy as np
from ndtiff import Dataset
import os


def test_v2_data(test_data_path):
    data_path = os.path.join(test_data_path, "v2", "ndtiffv2.0_test")
    dataset = Dataset(data_path)
    assert(np.mean(dataset.as_array()) > 0)

def test_v3_data(test_data_path):
    data_path = os.path.join(test_data_path, 'v3', 'ndtiffv3.0_test')
    dataset = Dataset(data_path)
    assert(np.mean(dataset.as_array()) > 0)
    for channel_name, correct_channel_name in zip(dataset.get_channel_names(), ['DAPI', 'FITC']):
        assert(channel_name == correct_channel_name)

def test_v2_stitched_data(test_data_path):
    data_path = os.path.join(test_data_path, "v2", "ndtiffv2.0_stitched_test")
    dataset = Dataset(data_path)
    stitched = np.array(dataset.as_array(stitched=True))
    assert(stitched[..., 0, 0] > 0)
    assert(stitched[..., -1, -1] > 0)
    assert(stitched[..., 0, -1] == 0)
    assert(stitched[..., -1, 0] == 0)

def test_v3_stitched_data(test_data_path):
    data_path = os.path.join(test_data_path, 'v3', 'ndtiffv3.0_stitched_test')
    dataset = Dataset(data_path)
    stitched = np.array(dataset.as_array(stitched=True, res_level=0))
    assert(stitched[..., 0, 0] > 0)
    assert(stitched[..., -1, -1] > 0)
    assert(stitched[..., 0, -1] == 0)
    assert(stitched[..., -1, 0] == 0)

def test_v3_magellan_explore(test_data_path):
    data_path = os.path.join(test_data_path, 'v3', 'Magellan_expolore_multi_channel')
    dataset = Dataset(data_path)
    stitched = np.array(dataset.as_array(stitched=True, res_level=0))
    
def test_v3_magellan_explore_negative_and_overwritten(test_data_path):
    data_path = os.path.join(test_data_path, 'v3', 'Magellan_expolore_negative_indices_and_overwritten')
    dataset = Dataset(data_path)
    stitched = np.array(dataset.as_array(stitched=True, res_level=0))

def test_v3_non_ctzp_axes(test_data_path):
    data_path = os.path.join(test_data_path, 'v3', 'Nonstandard_axis_names')
    dataset = Dataset(data_path)
    stitched = np.array(dataset.as_array())

def test_v3_mm_mda(test_data_path):
    data_path = os.path.join(test_data_path, 'v3', 'mm_mda_tcz_15')
    dataset = Dataset(data_path)
    stitched = np.array(dataset.as_array())

def test_v3_2_multichannel(test_data_path):
    data_path = os.path.join(test_data_path, 'v3', 'ndtiff3.2_multichannel')
    dataset = Dataset(data_path)
    assert(np.mean(dataset.read_image(channel='DAPI', time=0, z=1)) > 0)
    for channel_name, correct_channel_name in zip(dataset.get_channel_names(), ['DAPI', 'FITC']):
        assert(channel_name == correct_channel_name)

def test_v3_2_multichannel_axis_sorting(test_data_path):
    data_path = os.path.join(test_data_path, 'v3', 'ndtiff3.2_multichannel')
    dataset = Dataset(data_path)
    num_timepoints = len(dataset.axes['time'])
    num_channels = len(dataset.axes['channel'])
    num_slices = len(dataset.axes['z'])
    data = dataset.as_array(axes=None)
    assert data.shape[:-2] == (num_timepoints, num_channels, num_slices)

def test_v3_2_monochrome(test_data_path):
    data_path = os.path.join(test_data_path, 'v3', 'ndtiff3.2_monochrome')
    dataset = Dataset(data_path)
    assert (np.mean(dataset.read_image(time=0, z=1)) > 0)

def test_v3_2_magellan_rgb_explore(test_data_path):
    data_path = os.path.join(test_data_path, 'v3', 'ndtiff3.2_magellan_explore_rgb')
    dataset = Dataset(data_path)
    assert(np.sum(np.array(dataset.as_array(stitched=True)[0])) > 0)

def test_v3_2_11bit_data(test_data_path):
    data_path = os.path.join(test_data_path, 'v3', 'ndtiff3.2_11bit_1')
    dataset = Dataset(data_path)
    assert(dataset.read_metadata(time=0)['BitDepth'] == 11)
    assert(dataset.read_image(time=0).dtype == np.uint16)

def test_v3_2_12bit_pycromanager_data(test_data_path):
    data_path = os.path.join(test_data_path, 'v3', '12_bit_pycromanager_mda')
    dataset = Dataset(data_path)
    assert(dataset.read_image(time=0, channel='DAPI').dtype == np.uint16)

def test_v3_2_no_magellan_explore_channels(test_data_path):
    data_path = os.path.join(test_data_path, 'v3', 'no_magellan_explore_multi_channel')
    dataset = Dataset(data_path)
    assert(np.sum(np.array(dataset.as_array(stitched=True)[0])) > 0)

def test_v3_2_no_magellan_explore_no_channels(test_data_path):
    data_path = os.path.join(test_data_path, 'v3', 'no_magellan_explore_no_channel')
    dataset = Dataset(data_path)
    assert(np.sum(np.array(dataset.as_array(stitched=True)[0])) > 0)



