# NDTiffStorage
N-dimensional, multiresolution Tiff-based file format for Micro-Manager. Used by [Micro-Magellan](https://micro-manager.org/wiki/MicroMagellan) and [Pycro-Manager](https://github.com/micro-manager/pycro-manager). 

Pycro-Manager also contains [Python readers](https://pycro-manager.readthedocs.io/en/latest/apis.html#reading-acquired-data) for this format. 

## Specification

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
