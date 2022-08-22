# NDTiffStorage
NDTiffStorage is a file format for storing image data and metadata in a series of TIFF files, designed to scale to multi-Terabyte datasets collected at high speeds. It is the default saving format for [Micro-Magellan](https://micro-manager.org/wiki/MicroMagellan) and [Pycro-Manager](https://github.com/micro-manager/pycro-manager). The Java code for reading and writing these datasets is in this repository, and the [Python reader](https://pycro-manager.readthedocs.io/en/latest/apis.html#reading-acquired-data) can be found in Pycro-Manager.

It makes use of the portability of TIFF specification to be used across many applications, while also providing additional features that enable fast writing and reading of multi-Terabyte datasets in which images are indexed along arbitrary, N-dimensional axes (e.g. `{'time': 1, 'channel': 2}`). It is designed for flexibility, making no assumptions about the order or absecnce/presence of particular axis keys, and it allows for images of multiple sizes, bytes per pixel and RGB/grayscale in the same file. 

On top of this, it provides optional features to save data as a multiresolution pyramid. By laying out images in a 2D grid, large multiresolution pyramids can be generated that span multiple fields of view.

## Specification for NDTiff v2

### Directory layout
The directory layout structure of an NDTiff dataset is as follows:
```

└── Full resolution
│   ├── NDTiff.index
│   ├── {name}_NDTiffStack.tif
│   └── {name}_NDTiffStack_1.tif
└── display_settings.txt

```
Without the optional multiresolution features turned on, all the data will be contained in a folder called `Full resolution`. Datasets over 4GB will contain multiple TIFF files, since the TIFF specification maxes out at 4GB per file. Succesive files will have numbers appended to the end (e.g. `_1`, `_2`, ...)

If the multi-resolution pyramid features are being used, this directory structure will be repeated with a new folder for each resolution level of the pyramid:
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

#### Structure within each TIFF file

The individual TIFF files are standard TIFF files, with the addition of a specialized header:

*8 bytes*: Standard TIFF Header

*4 bytes*: 32-bit integer containing the number 483729, added in v1.0 to differentiate from Micro-Manager multipage tiff files

*4 bytes*: 32-bit integer containing major version (added in v1.0)

*4 bytes*: 32-bit integer containing minor version (added in v3.0)

*4 bytes*: 32-bit integer containing Summary metadata header, 2355492 

*4 bytes*: 32-bit integer contianing K, the length of the summary metadata

*K bytes*: JSON-serialized summary metadata, stored at UTF8 text

#### The NDTiff.index file

The index file is what enables the formats fast performance. Since a vanilla Tiff file doesn't contain any sort of table of contents, the entire file must be read through in order to discover the locations of all image data. To avoid the large performance penality that would come with this, the index file instead contains all the information needed to access data anywhere in the file in a concise form. The index file contains a series of entries (determined by the order images were saved), with each entry corresponding to a single image+metadata. It is structured as follows:

*4 bytes:* (32 bit integer) containing the number K (length of next field)

*K bytes:* JSON-serialized UTF8 text of the "axes" object identifying the image (e.g. `{'time': 1, 'channel: 2'`)

*4 bytes:* (32 bit integer) containing the number N (length of next field)

*N bytes:* UTF8 string of the filename within the dataset where the image and metadata are

*4 bytes:* (32 bit unsigned integer) Byte offset of the image pixels

*4 bytes:* (32 bit integer) Width of the image (assumed to be 2D)

*4 bytes:* (32 bit integer) Height of the image

*4 bytes:* (32 bit integer) Pixel type. 0 = 8bit; 1= 16bit; 2=8bitRGB

*4 bytes:* (32 bit integer) Pixel compression. Currently only 0 (Uncompressed) defined

*4 bytes:* (32 bit unsigned integer) Byte offset of the metadata, wich is UTF8 encoded serialized JSON

*4 bytes:* (32 bit integer) Length of metadata

*4 bytes:* (32 bit integer) Metadata compression. Currently only 0 (Uncompressed) defined

#### The display_settings.txt file

This file is optional, and contains settings for displaying the data contained within the file (colormaps, contrast settings, etc.). No particular form is assumed, other than it is all contained in JSON.


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

- 4 bytes containg the offset of the next IFD (per the TIFF specification)
- The pixel data
- In RGB files only, 6 bytes containing the values of the BitsPerSample tag Pixel values
- 16 bytes containing the values of the XResolution and YResolution tags
- The value of the MicroManagerMetadata tag: image metadata (UTF-8 JSON) 


### Index map

A listing of all the images contained in the file and their byte offsets. This allows a specific image to be quickly accessed without having to parse the entire file and read in image metadata. It consists of the following:

- A 4 byte header (3453623 = 0x0034b2b7)
- 4 bytes containing the number of entries in the index map
- 20 bytes for each entry, with 4 bytes each allocated to the image’s channel index, z index, frame index, position index, and byte offset of the image’s IFD within the file. Since this format supports storage of arbitrary named axes (i.e. not just `channel`, `z`, `time`, `position`), all named axes other than these 4 are translated into a four axis coordinate. This is done by assigning a unique channel index to every unique combination of axes present (after excluding `z`, `time`,   and `position`). Decoding this mapping requires reading the image metadata of all such combinations.


### Image display settings

Image display settings (channel contrast and colors). The first 4 bytes of this block contain the Display Settings Header (347834724 = 0x14BB8964), and the next 4 contain the number of subsequent bytes reserved for display settings. A UTF-8 JSON string containing display settings is written.
