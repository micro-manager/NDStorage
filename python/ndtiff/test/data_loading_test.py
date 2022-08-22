import numpy as np
from ndtiff import Dataset

data_path = "/Users/henrypinkard/Desktop/ndtiffv2.0_test"
# data_path = "/Users/henrypinkard/Desktop/ndtiffv2.0_stitched_test"

# open the dataset
dataset = Dataset(data_path)

# read tiles or tiles + metadata by channel, slice, time, and position indices
# img is a numpy array and md is a python dictionary
# img, img_metadata = dataset.read_image(l=10, read_metadata=True)

dask_array = dataset.as_array(stitched=False)
print(np.array(dask_array))

pass

#TODO: Generate new type datasets and write a few automated tests of key features