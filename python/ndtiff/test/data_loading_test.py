import numpy as np
from ndtiff import Dataset

def test_v2_data():
    data_path = "/Users/henrypinkard/Desktop/ndtiffv2.0_test"
    dataset = Dataset(data_path)
    assert(np.mean(dataset.as_array()) > 0)

def test_v3_data():
    data_path = '/Users/henrypinkard/Desktop/ndtiffv3.0_test'
    dataset = Dataset(data_path)
    assert(np.mean(dataset.as_array()) > 0)

def test_v2_stitched_data():
    data_path = "/Users/henrypinkard/Desktop/ndtiffv2.0_stitched_test"
    dataset = Dataset(data_path)
    stitched = np.array(dataset.as_array(stitched=True))
    assert(stitched[..., 0, 0] > 0)
    assert(stitched[..., -1, -1] > 0)
    assert(stitched[..., 0, -1] == 0)
    assert(stitched[..., -1, 0] == 0)

def test_v3_stitched_data():
    data_path = '/Users/henrypinkard/Desktop/ndtiffv3.0_stitched_test'
    dataset = Dataset(data_path)
    stitched = np.array(dataset.as_array(stitched=True))
    assert(stitched[..., 0, 0] > 0)
    assert(stitched[..., -1, -1] > 0)
    assert(stitched[..., 0, -1] == 0)
    assert(stitched[..., -1, 0] == 0)


