import numpy as np
import os
import shutil
from ndtiff import Dataset
from ndtiff.ndtiff_file import _SinlgeNDTiffWriter, _SingleNDTiffReader

def test_write_single_file(test_data_path):
    """
    Create a single NDTiff file and read it back in
    """
    data_path = os.path.join(test_data_path, 'writer_tests')
    # make sure the directory exists
    os.makedirs(data_path, exist_ok=True)
    # delete any existing files
    for f in os.listdir(data_path):
        os.remove(os.path.join(data_path, f))
    filename = 'test_write_single_file.tiff'
    writer = _SinlgeNDTiffWriter(data_path, filename, summary_md={})

    rgb = False
    image_height = 256
    image_width = 256
    bit_depth = 16
    pixels = np.arange(image_height * image_width, dtype=np.uint16).reshape((image_height, image_width))

    index_key = frozenset({'time': 0})
    index_entry = writer.write_image(index_key, pixels, {}, rgb, image_height, image_width, bit_depth)
    writer.finished_writing()

    # read the file back in
    single_reader = _SingleNDTiffReader(os.path.join(data_path, filename))

    single_reader.read_image(index_entry)

    # delete writer tests directory
    shutil.rmtree(data_path)