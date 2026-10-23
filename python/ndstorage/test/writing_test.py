import numpy as np
import os
import shutil
from ..ndtiff_file import SingleNDTiffWriter, SingleNDTiffReader
from ..ndtiff_dataset import NDTiffDataset
from ..ndram_dataset import NDRAMDataset
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
    Write an NDTiff dataset and read it back in, testing pixels and metadata
    """
    full_path = os.path.join(test_data_path, 'test_write_full_dataset')
    dataset = NDTiffDataset(full_path, summary_metadata={}, writable=True)

    image_height = 256
    image_width = 256
    images = []
    for time in range(10):
        pixels = np.ones(image_height * image_width, dtype=np.uint16).reshape((image_height, image_width)) * time
        images.append(pixels)
    for time in range(10):
        axes = {'time': time}
        dataset.put_image(axes, images[time], {'time_metadata': time})

    dataset.finish()

    # read the file back in
    dataset = NDTiffDataset(full_path)
    for time in range(10):
        pixels = np.ones(image_height * image_width, dtype=np.uint16).reshape((image_height, image_width)) * time
        axes = {'time': time}
        read_image = dataset.read_image(**axes)
        assert np.all(read_image == pixels)
        assert dataset.read_metadata(**axes) == {'time_metadata': time}

def test_write_full_dataset_8_bit(test_data_path):
    """
    Write an NDTiff dataset and read it back in, testing pixels and metadata
    """
    full_path = os.path.join(test_data_path, 'test_write_full_dataset')
    dataset = NDTiffDataset(full_path, summary_metadata={}, writable=True)

    image_height = 256
    image_width = 256
    images = []
    for time in range(10):
        pixels = np.ones(image_height * image_width, dtype=np.uint16).reshape((image_height, image_width)) * time
        images.append(pixels)
    for time in range(10):
        axes = {'time': time}
        dataset.put_image(axes, images[time], {'time_metadata': time})

    dataset.finish()

    # read the file back in
    dataset = NDTiffDataset(full_path)
    for time in range(10):
        pixels = np.ones(image_height * image_width, dtype=np.uint8).reshape((image_height, image_width)) * time
        axes = {'time': time}
        read_image = dataset.read_image(**axes)
        assert np.all(read_image == pixels)
        assert dataset.read_metadata(**axes) == {'time_metadata': time}

def test_write_full_dataset_RAM():
    dataset = NDRAMDataset()

    image_height = 256
    image_width = 256
    images = []
    for time in range(10):
        pixels = np.ones(image_height * image_width, dtype=np.uint16).reshape((image_height, image_width)) * time
        images.append(pixels)
    for time in range(10):
        axes = {'time': time}
        dataset.put_image(axes, images[time], {'time_metadata': time})

    dataset.finish()

    for time in range(10):
        pixels = np.ones(image_height * image_width, dtype=np.uint16).reshape((image_height, image_width)) * time
        axes = {'time': time}
        read_image = dataset.read_image(**axes)
        assert np.all(read_image == pixels)
        assert dataset.read_metadata(**axes) == {'time_metadata': time}





@pytest.mark.parametrize("overlap", [(10, 10), (0, 10), (10, 0), (1, 3), (0, 0)])
def test_stitched_overlap(test_data_path, overlap):
    """
    Stitch a 2x2 grid of tiles whose overlap may be zero along one axis
    """
    overlap_y, overlap_x = overlap
    full_path = os.path.join(test_data_path, 'test_stitched_overlap')
    dataset = NDTiffDataset(full_path, summary_metadata={'GridPixelOverlapY': overlap_y,
                                                         'GridPixelOverlapX': overlap_x},
                            writable=True)
    image_height = 64
    image_width = 80
    for row in range(2):
        for column in range(2):
            pixels = np.full((image_height, image_width), 1 + 2 * row + column, dtype=np.uint16)
            dataset.put_image({'row': row, 'column': column}, pixels, {})
    dataset.finish()

    dataset = NDTiffDataset(full_path)
    dataset._full_resolution = True
    stitched = dataset.as_array(stitched=True)
    tile_height = image_height - overlap_y
    tile_width = image_width - overlap_x
    expected = np.repeat(np.repeat(np.array([[1, 2], [3, 4]], dtype=np.uint16),
                                   tile_height, axis=0), tile_width, axis=1)
    assert stitched.shape == expected.shape
    assert np.array_equal(np.asarray(stitched), expected)
    dataset.close()
