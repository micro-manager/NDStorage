import numpy as np
import os
import shutil
import threading
import time
import sys
from collections import deque
#from ..ndtiff_file import SingleNDTiffWriter, SingleNDTiffReader
from ..ndtiff_dataset import NDTiffDataset
#from ..ndram_dataset import NDRAMDataset
import pytest

@pytest.fixture(scope="function")
def test_data_path(tmp_path_factory):
    data_path = tmp_path_factory.mktemp('writer_tests')
    for f in os.listdir(data_path):
        os.remove(os.path.join(data_path, f))
    yield str(data_path)
    shutil.rmtree(data_path)

# loop for threaded writing
def image_write_loop(my_deque: deque, dataset: NDTiffDataset, run_event: threading.Event):
    while run_event.is_set() or len(my_deque) != 0:
        try:
            if my_deque:
                current_time, pixels = my_deque.popleft()
                axes = {'time': current_time}
                dataset.put_image(axes, pixels, {'time_metadata': current_time})
            else:
                time.sleep(0.001)
        except IndexError:
            break

def test_write_full_dataset_multithreaded(test_data_path):
    """
    Write an NDTiff dataset and read it back in, testing pixels and metadata
    """
    assert sys.version_info[0] >= 3, "For test_write_full_dataset_multithreaded Python >= 3.13 is recommended"
    assert sys.version_info[1] >= 13, "For test_write_full_dataset_multithreaded Python >= 3.13 is recommended"

    full_path = os.path.join(test_data_path, 'test_write_full_dataset')
    dataset = NDTiffDataset(full_path, summary_metadata={}, writable=True, pixel_compression=8)
    image_deque = deque()
    run_event = threading.Event()
    run_event.set()

    image_height = 256
    image_width = 256

    thread = threading.Thread(target=image_write_loop, args=(image_deque, dataset, run_event))
    thread.start()

    time_counter = 0
    time_limit = 10

    while True:
        if len(image_deque) < 4:
            pixels = np.ones(image_height * image_width, dtype=np.uint16).reshape((image_height, image_width)) * time_counter
            image_deque.append((time_counter, pixels))
            time_counter += 1
            if time_counter >= time_limit:
                break
        else:
            time.sleep(0.001) # if the deque is full, wait a bit
    
    run_event.clear()
    thread.join()
    dataset.finish()

    # read the file back in
    dataset = NDTiffDataset(full_path)
    for time_index in range(time_limit):
        pixels = np.ones(image_height * image_width, dtype=np.uint16).reshape((image_height, image_width)) * time_index
        axes = {'time': time_index}
        read_image = dataset.read_image(**axes)
        assert np.all(read_image == pixels)
        assert dataset.read_metadata(**axes) == {'time_metadata': time_index}

