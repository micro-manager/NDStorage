# import numpy as np
# import os
# import shutil
# from ndtiff.ndtiff_file import SingleNDTiffWriter, SingleNDTiffReader
# from ndtiff.ndtiff_dataset import NDTiffDataset
# import pytest
# import time
# from tqdm import tqdm
#
# def writing_speed_test(test_data_path):
#     """
#     Test the speed of writing a large dataset
#     """
#     full_path = os.path.join(test_data_path, 'test_write_speed')
#     dataset = NDTiffDataset(full_path, summary_metadata={}, name='prefix')
#
#     image_height = 2048
#     image_width = 2028
#     N_images = 1000
#     image = np.random.randint(0, 2**16, size=(image_height, image_width), dtype=np.uint16)
#     MB_per_image = image_height * image_width * 2 / 1024 / 1024
#
#     image_write_times = []
#     start_time = time.time()
#     for t in tqdm(range(N_images)):
#         image_time = time.time()
#         axes = {'time': t}
#         dataset.put_image(axes, image, {'time_metadata': t})
#         image_write_times.append(time.time() - image_time)
#     dataset.finished_writing()
#
#     elapsed_time = time.time() - start_time
#     print(f"Time to write {N_images} images: ", elapsed_time,
#           "\nspeed: ", N_images * MB_per_image / elapsed_time, "MB/s")
#
#     return image_write_times, MB_per_image
#
#
# full_path = os.path.join('.', 'top_level')
#
# os.makedirs(full_path, exist_ok=True)
# for f in os.listdir(full_path):
#     os.remove(os.path.join(full_path, f))
#
# image_write_times, MB_per_image = writing_speed_test(full_path)
#
# MB_per_second_times = [MB_per_image / t for t in image_write_times]
#
# import matplotlib.pyplot as plt
# fig, ax = plt.subplots()
# ax.semilogy(MB_per_second_times)
# ax.set_xlabel('Image number')
# ax.set_ylabel('Speed (MB/s)')
# plt.show()
#
# shutil.rmtree(full_path)
