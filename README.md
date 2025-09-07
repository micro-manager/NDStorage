[![License](https://img.shields.io/pypi/l/ndstorage.svg)](https://github.com/micro-manager/ndstorage/raw/master/LICENSE)
[![PyPI](https://img.shields.io/pypi/v/ndstorage.svg)](https://pypi.org/project/ndstorage)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/ndstorage.svg)](https://pypistats.org/packages/ndstorage)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.7065552.svg)](https://doi.org/10.5281/zenodo.7065552) (Test datasets)


# NDStorage
This repository contains APIs for storing and retrieving image datasets indexed along N-dimensional axes (e.g. time, channel, x, y, z, etc). It also contains two implementations of such N-dimensional data storage packages: `NDRAM` and `NDTiff`. `NDRAM` is simply an in-memory storage for image data that implements the NDStorage API. 

NDTiff is a file format for storing image data and metadata in a series of TIFF files, designed to scale to multi-Terabyte datasets collected at high speeds. It is the default saving format for [Pycro-Manager](https://github.com/micro-manager/pycro-manager) and one of three available options in Micro-Manager. This repository contains Python and Java code for reading/writing these files. Instructions on how to use the python readers can be found in the [Pycro-Manager documentation](https://pycro-manager.readthedocs.io/en/latest/apis.html#reading-acquired-data).

Briefly, reading ndtiff datasets can be performed with:
```
pip install ndstorage
```
```python
from ndstorage import Dataset
data = Dataset('/path/to/data')
```

or to write a new dataset:

```python
import numpy as np
from ndstorage import NDTiffDataset

summary_metadata = {'name_1': 123, 'name_2': 'something else'} # make this whatever you want
dataset = NDTiffDataset('your/data/path', summary_metadata=summary_metadata, writable=True)

image_height = 256
image_width = 256

for time in range(10):
    # generate random image
    pixels = np.random.randint(0, 2 ** 16, (image_height, image_width), dtype=np.uint16)
    
    # a dict with strings as keys and strings or ints as values that uniquely identifies this image
    image_coordinates = {'time': time, 'other_axis_name': 4} 
    image_metadata = {'name_1': 123, 'name_2': 'something'} # whatever you want
    dataset.put_image(image_coordinates, pixels, image_metadata)

dataset.finish()
```

## Support for Cloud-based IO
The python `ndstorage` package supports IO to cloud services (i.e. AWS s3) via thr `file_io` submodule.
you will need to provide your own file interface methods (or use those from other packages i.e. `boto3`), in the following way:
```
from _your_custom_storage_module_ import storage
import ndstorage

file_io = ndstorage.file_io.NDTiffFileIO(open_function=storage.open,
                                      listdir_function=storage.listdir,
                                      path_join_function=storage.pathjoin, 
                                      isdir_function=storage.isdir)

dset = ndstorage.Dataset("s3://bucket_name/path/to/your/dataset", file_io=file_io)
```

## Rationale for NDTiff
The NDTiff format is optimized for size, speed, and flexibility. Several optimizations are in place to achieve high-performance in each category.

**Size**. In a traditional TIFF, the locations of the pixels of each image are stored in headers (called "Image File Directories") that are dispersed throughout the file. The leads to major performance bottlenecks on large files, since identifying the location of a particular image may require scanning through many headers dispersed throughout the file. To avoid this limitation, NDTiff writes a separate `NDTiff.index` which contains the locations an essential metadata of each image over one or more TIFF files. This enables the entire dataset to be queried and individual images retrieved extremely quickly. Because of presence of this index, the native TIFF headers are not strictly necessary, however, they do allow the datasets to be opened (less efficiently) by standard TIFF readers, as well as recovery mechanism in case of loss of the index file.

**Speed**. One of the major performance bottlenecks for streaming data to disk at high speeds is the creation of new files. Each time this happens, there is a performance penalty from making operating system calls. Thus, speed can be increased by putting many images into the same file. NDTiff uses individual TIFFs of 4GB apiece (the maximum allowable TIFF size), along with many internal optimizations to increase the speed with which data can be written.

**Flexibility**. NDTiff does not assume any particular model of the dataset, aside from the fact that it can be broken into multiple 2D images. Each 2D image is indexed by a position along one or more axes (e.g. `{'time': 1, 'channel': 2}`). The choice of number of axes is arbitrary and up to the user. This means the format can equally well store many modalities that use multiple images including 3D data, time series, hyperspectral data, high-dynamic range, etc.



## Multi-resolution pyramids
An additional feature of this library is the use [multi-resolution pyramids](https://en.wikipedia.org/wiki/Pyramid_%28image_processing%29). Having the data in this form is very useful for image processing and visualization of extremely large images, because the data can be visualized/analyzed at multiple scales without having to pull the entire, full-resolution image into memory. This is implemented by using multiple parallel NDTiffStorage datasets, each of which corresponds to one resolution level. It assumes multiple images are laid out in regular an XY grid, and downsamples along these X and Y dimensions.

## MATLAB Support
For users who need to utilize MATLAB-based image analysis tools, a dedicated importer for MATLAB is available from https://github.com/dickinson-lab/NDTiff_MATLAB. It loads multi-dimensional datasets into intuitively-shaped 4-D or 5-D matrices, supports accessing only desired regions of the data, and provides access to the summary metadata. 

# Specification for NDTiff v3.x

NDTiff datasets are a folder that comprises three types of files:

```
├── NDTiff.index
├── {name}_NDTiffStack.tif
├── {name}_NDTiffStack_1.tif
└── display_settings.txt
```

The `NDTiff.index` file contains information about where all the different images live in each file and what their keys are (e.g. `{'time': 1, 'channel': 2}`). As of version NDTiff v3.2, these keys can take values as positive or negative integers or strings. For example, `{'time': -1, 'channel': 'GFP'}`.


 The `{name}_NDTiffStack.tif` contains image data and metadata. Datasets over 4GB will contain multiple TIFF files, since the TIFF specification maxes out at 4GB per file. Successive files will have numbers appended to the end (e.g., `_1`, `_2`, ...). Each image has one corresponding JSON object containing metadata, and there is one "summary metadata" JSON object that describes the whole dataset. The summary metadata object is replicated across each file. `display_settings.txt` is an optional JSON file containing contrast and display settings, with no particular assumed structure.


## Multi-resolution pyramid
The multi-resolution pyramid versions of the files are essentially series of individual NDTiff datasets, each of which has been downsampled by a factor of 2 along XY:

```
├── Downsampled_x4
│   ├── NDTiff.index
│   └── {name}_NDTiffStack.tif
├── Downsampled_x2
│   ├── NDTiff.index
│   └── {name}_NDTiffStack.tif
├── Full resolution
│   ├── NDTiff.index
│   └── {name}_NDTiffStack.tif
└── display_settings.txt
```

In addition, with each successive downsampling, a 2x2 grid of adjacent tiles are merged into a a single tile. This keeps the size of the images saved equal at each successive resolution level.

### Structure within each TIFF file

The individual TIFF files are standard TIFF files, with the addition of a specialized header:

*8 bytes*: Standard TIFF Header

*4 bytes*: 32-bit integer containing the number 483729, added in v1.0 to differentiate from Micro-Manager multipage tiff files

*4 bytes*: 32-bit integer containing major version (added in v1.0)

*4 bytes*: 32-bit integer containing minor version **(added in v3.0)**

*4 bytes*: 32-bit integer containing Summary metadata header, 2355492 

*4 bytes*: 32-bit integer containing K, the length of the summary metadata

*K bytes*: JSON-serialized summary metadata, stored at UTF8 text

#### The NDTiff.index file

The index file is what enables the formats fast performance. Since a vanilla Tiff file doesn't contain any sort of table of contents, the entire file must be read through in order to discover the locations of all image data. To avoid the large performance penalty that would come with this, the index file instead contains all the information needed to access data anywhere in the file in a concise form. The index file contains a series of entries (determined by the order images were saved), with each entry corresponding to a single image+metadata. It is structured as follows:

*4 bytes:* (32-bit integer) containing the number K (length of next field)

*K bytes:* JSON-serialized UTF8 text of the "axes" object identifying the image (e.g. `{'time': 1, 'channel: 2'`)

*4 bytes:* (32-bit integer) containing the number N (length of next field)

*N bytes:* UTF8 string of the filename within the dataset where the image and metadata are

*4 bytes:* (32-bit unsigned integer) Byte offset of the image pixels

*4 bytes:* (32-bit integer) Width of the image (assumed to be 2D)

*4 bytes:* (32-bit integer) Height of the image

*4 bytes:* (32-bit integer) Pixel type. 
    0:  8bit monochrome 
    1:  16bit monochrome
    2:  8bit RGB
    3:  10bit monochrome
    4:  12bit monochrome
    5:  14bit monochrome

*4 bytes:* (32-bit integer) Pixel compression. Currently only 0 (Uncompressed) defined

*4 bytes:* (32-bit unsigned integer) Byte offset of the metadata, which is UTF8 encoded serialized JSON

*4 bytes:* (32-bit integer) Length of metadata

*4 bytes:* (32-bit integer) Metadata compression. Currently only 0 (Uncompressed) defined

#### The display_settings.txt file

This file is optional and contains settings for displaying the data contained within the file (colormaps, contrast settings, etc.). No particular form is assumed, other than it is all contained in JSON.

## Revision history

### 3.3

Added new pixel types (10, 12, and 14 bit monochrome)

### 3.2

Added the ability to use String values axis keys, e.g. `{'time': 0, 'channel': 'GFP', 'camera': 'Left'}` 

### 3.1

Fixes a bug where metadata was sometimes encoded using an incorrect encoding, see https://github.com/micro-manager/pycro-manager/issues/467#issuecomment-1354154902

### 3.0

1. In version 2 NDTiff files, even when not using multi-resolution pyramid features, the data were in a `Full resolution` directory. In v3.0 this was eliminated in favor of putting them directly in the top-level directory
2. An additional 4 bytes was added to bytes 16-20 of the header of each file containing the minor version of the format, thereby shifting back the summary metadata and it's header by 4 bytes

## (DEPRECATED) Specification for NDTiff v1 and earlier

This section contains the now deprecated description of NDTiff 1.0 files

### Header

- Bytes 0-7 (0x0-0x7): Standard TIFF Header
- 8-11 (0x8-0xb): Index map offset header (54773648 = 0x0343C790)
- 12-15 (0xc-0xf): Index map offset
- 16-19 (0x10-0x13): Display settings offset header (483765892 = 0x1CD5AE84)
- 20-23 (0x14-0x17): Display settings offset
- 24-27 (0x18-0x1b): The number 483729, added in v1.0.0 to differentiate from Micro-Manager multipage tiff files
- 28-31 (0x1c-0x1f): Major version (added in 1.0.0)
- 32-35 (0x20-0x23): Summary metadata header (2355492 = 0x0023F124)
- 36-39 (0x24-0x27): Summary metadata length
- 40- (0x28-): summary metadata (UTF-8 JSON)


### Image File Directories

The first IFD starts immediately after the summary metadata. The tags are written in the following order (non-standard TIFF tags have the values listed after them), following the TIFF specification requirement that they be sorted numerically:

- ImageWidth (256 = 0x0100)
- ImageHeight (257 = 0x0101)
- BitsPerSample (258 = 0x0102)
- Compression (259 = 0x0103)
- PhotometricInterpretation (262 = 0x0106)
- StripOffsets (273 = 0x0111)
- SamplesPerPixel (277 = 0x0115)
- RowsPerStrip (278 = 0x0116)
- StripByteCounts (279 = 0x0117)
- XResolution (282 = 0x011a)
- YResolution (283 = 0x011b)
- ResolutionUnit (296 = 0x0128)
- IJMetadataByteCounts (first IFD only) (50838 = 0xc696)
- IJMetadata (first IFD only) (50839 = 0xc697)
- MicroManagerMetadata (51123 = 0xc7b3)


#### Immediately after these tags are written:

- 4 bytes containing the offset of the next IFD (per the TIFF specification)
- The pixel data
- In RGB files only, 6 bytes containing the values of the BitsPerSample tag Pixel values
- 16 bytes containing the values of the XResolution and YResolution tags
- The value of the MicroManagerMetadata tag: image metadata (UTF-8 JSON) 


### Index map

A listing of all the images contained in the file and their byte offsets. This allows a specific image to be quickly accessed without having to parse the entire file and read in image metadata. It consists of the following:

- A 4-byte header (3453623 = 0x0034b2b7)
- 4 bytes containing the number of entries in the index map
- 20 bytes for each entry, with 4 bytes each allocated to the image’s channel index, z index, frame index, position index, and byte offset of the image’s IFD within the file. Since this format supports storage of arbitrary named axes (i.e., not just `channel`, `z`, `time`, `position`), all named axes other than these 4 are translated into a four-axis coordinate. This is done by assigning a unique channel index to every unique combination of axes present (after excluding `z`, `time`, and `position`). Decoding this mapping requires reading the image metadata of all such combinations.


### Image display settings

Image display settings (channel contrast and colors). The first 4 bytes of this block contain the Display Settings Header (347834724 = 0x14BB8964), and the next 4 contain the number of subsequent bytes reserved for display settings. A UTF-8 JSON string containing display settings is written.



