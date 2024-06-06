import numpy as np
import os
import shutil
from ndtiff.ndtiff_file import SingleNDTiffWriter, SingleNDTiffReader
from ndtiff.ndtiff_dataset import NDTiffDataset
import pytest

@pytest.fixture(scope="function")
def test_data_path(tmp_path_factory):
    data_path = tmp_path_factory.mktemp('writer_tests')
    for f in os.listdir(data_path):
        os.remove(os.path.join(data_path, f))
    yield str(data_path)
    shutil.rmtree(data_path)

def test_write_single_file(test_data_path):
    """
    Create a single NDTiff file and read it back in
    """
    filename = 'test_write_single_file.tif'
    writer = SingleNDTiffWriter(test_data_path, filename, summary_md={})

    image_height = 256
    image_width = 256
    pixels = np.arange(image_height * image_width, dtype=np.uint16).reshape((image_height, image_width))

    index_key = frozenset({'time': 0}.items())
    index_entry = writer.write_image(index_key, pixels, {})
    writer.finished_writing()

    # read the file back in
    single_reader = SingleNDTiffReader(os.path.join(test_data_path, filename))
    read_pixels = single_reader.read_image(index_entry)
    assert np.all(read_pixels == pixels)


def test_write_full_dataset(test_data_path):
    """
    Write an NDTiff dataset
    """
    full_path = os.path.join(test_data_path, 'test_write_full_dataset')
    dataset = NDTiffDataset(full_path, summary_metadata={})

    image_height = 256
    image_width = 256
    images = []
    for time in range(10):
        pixels = np.ones(image_height * image_width, dtype=np.uint16).reshape((image_height, image_width)) * time
        images.append(pixels)
    for time in range(10):
        axes = {'time': time}
        dataset.put_image(axes, images[time], {})

    dataset.finished_writing()

    # read the file back in
    dataset = NDTiffDataset(full_path)
    for time in range(10):
        pixels = np.ones(image_height * image_width, dtype=np.uint16).reshape((image_height, image_width)) * time
        axes = {'time': time}
        read_image = dataset.read_image(**axes)
        assert np.all(read_image == pixels)
