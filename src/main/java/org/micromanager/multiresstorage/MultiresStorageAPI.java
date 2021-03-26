package org.micromanager.multiresstorage;

import mmcorej.TaggedImage;

import java.awt.*;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.Future;

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
    * a larger stitched image. Must have entries for "row" and "column" in axes.
    * Blocks if writing queue is full
    * @param ti
    * @param axes
    * @param rgb
    * @param imageHeight
    * @param imageWidth
    * @return
    */
   public Future putImageMultiRes(TaggedImage ti, HashMap<String, Integer> axes,
                           boolean rgb, int imageHeight, int imageWidth) ;

   /**
    * return number of resolutions of the multiresolution pyramid
    * @return
    */
   public int getNumResLevels();

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
   public TaggedImage getDisplayImage(HashMap<String, Integer> axes,
                                      int resIndex,
                                      int xOffset, int yOffset,
                                      int imageWidth, int imageHeight);


   /**
    * Get a single image from multi-resolution dataset at the given axis
    *
    * @param axes
    * @param resolutionIndex 0 is full resolution, 1 is downsampled x2, 2 is downsampled x4, etc
    * @return
    */
   public TaggedImage getImage(HashMap<String, Integer> axes, int resolutionIndex);

}
