package org.micromanager.multiresstorage;

import mmcorej.TaggedImage;

import java.awt.*;
import java.util.HashMap;
import java.util.Set;

/**
 * API of storage that includes multi-resolution support, in addition to the base features
 * In StorageAPI.java. This API expects images to be laid out in XY tiles on a regular grid.
 * Images should be added based on integer row and column indices (which can be negative),
 * and can be accessed either based on these row/col indices or as a contiguous region with
 * multiple tiles automatically stitched together. Successive resolutions are assumed to be
 * downsampled by a factor of 2. The number of resolutions can be changed dynamically, and
 * is an implementation detail of the underlying classes
 *
 */
public interface MultiresStorageAPI extends StorageAPI{

   /**
    * Get a set of the (row, col) indices at which data has been acquired at this
    * @param zIndex
    * @return
    */
   public Set<Point> getTileIndicesWithDataAt(int zIndex);

   /**
    * Add an image into storage, which corresponds to a particular row/column in
    * a larger stitched image
    *
    * @param taggedImg
    * @param axes
    * @param row
    * @param col
    */
   public void putImage(TaggedImage taggedImg, HashMap<String, Integer> axes, int row, int col);

   /**
    * return number of resolutions of the multiresolution pyramid
    * @return
    */
   public int getNumResLevels();

   /**
    * Get a single tile of a multiresolution stitched dataset. This is called from Python code
    *
    * @param axes HashMap mapping axis names to positions
    * @param resIndex 0 is full resolution, 1 is downsampled x2, 2 is downsampled x4, etc
    * @param row row index of tile in the requested resolution
    * @param col column index of tile in the requested resolution
    * @return
    */
   public TaggedImage getTileByRowCol(HashMap<String, Integer> axes, int resIndex, int row, int col);

   /**
    * Check for a  tile of a multiresolution stitched dataset. This is called from Python code
    *
    * @param axes HashMap mapping axis names to positions
    * @param resIndex 0 is full resolution, 1 is downsampled x2, 2 is downsampled x4, etc
    * @param row row index of tile in the requested resolution
    * @param col column index of tile in the requested resolution
    * @return
    */
   public boolean hasTileByRowCol(HashMap<String, Integer> axes, int resIndex, int row, int col);

   /**
    * Get a single stitched image that spans multiple tiles
    *
    * @param axes HashMap mapping axis names to positions
    * @param resIndex 0 is full resolution, 1 is downsampled x2, 2 is downsampled x4, etc
    * @param xOffset leftmost pixel in the requested resolution level
    * @param yOffset topmost pixel in the requested resolution level
    * @param imageWidth width of the returned image
    * @param imageHeight height of the returned image
    * @return
    */
   public TaggedImage getStitchedImage(HashMap<String, Integer> axes,
                                       int resIndex,
                                       int xOffset, int yOffset,
                                       int imageWidth, int imageHeight);

   /**
    * Check if dataset has an image with the specified axes
    *
    * @param axes HashMap mapping axis names to positions
    * @param resolutionIndex 0 is full resolution, 1 is downsampled x2, 2 is downsampled x4, etc
    * @return
    */
   public boolean hasImage(HashMap<String, Integer> axes, int resolutionIndex);


   /**
    * Get a single image from multi-resolution dataset at the given axis
    *
    * @param axes
    * @param resolutionIndex 0 is full resolution, 1 is downsampled x2, 2 is downsampled x4, etc
    * @return
    */
   public TaggedImage getImage(HashMap<String, Integer> axes, int resolutionIndex);

}
