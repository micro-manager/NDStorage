package org.micromanager.ndtiffstorage;

import mmcorej.TaggedImage;
import mmcorej.org.json.JSONObject;

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
public interface MultiresNDTiffAPI extends NDTiffAPI {

   /**
    * Get a set of the (row, col) indices at which data has been acquired at this
    * @param zName name of the xis
    * @param zIndex
    * @return
    */
   public Set<Point> getTileIndicesWithDataAt(String zName, int zIndex);

   /**
    * Use getTileIndicesWithDataAt(String zName, int zIndex) instead
    * @param zIndex
    * @return
    */
   @Deprecated
   public Set<Point> getTileIndicesWithDataAt(int zIndex);

   /**
    * Add an image into storage, which corresponds to a particular row/column in
    * a larger stitched image. Must have entries for "row" and "column" in axes.
    * Blocks if writing queue is full
    * @param pixels
    * @param metadata
    * @param axes
    * @param rgb
    * @param bitDepth
    * @param imageHeight
    * @param imageWidth
    * @return
    */
   public Future<IndexEntryData> putImageMultiRes(Object pixels, JSONObject metadata, HashMap<String, Object> axes,
                                  boolean rgb, int bitDepth, int imageHeight, int imageWidth) ;

   /**
    * return number of resolutions of the multiresolution pyramid
    * @return
    */
   public int getNumResLevels();

   /**
    * Increase the number of resolutions in the pyramid. Needed if the UI wants to
    * Zoom out further than the current max resolution level
    * @param newMaxResolutionLevel
    */
   public void increaseMaxResolutionLevel(int newMaxResolutionLevel);

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
   public TaggedImage getDisplayImage(HashMap<String, Object> axes,
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
   public TaggedImage getImage(HashMap<String, Object> axes, int resolutionIndex);


   /**
    * Check if dataset has an image with the specified axes
    *
    * @param axes HashMap mapping axis names to positions
    * @param resolutionIndex 0 is full resolution, 1 is downsampled x2, 2 is downsampled x4, etc
    * @return
    */
    public boolean hasImage(HashMap<String, Object> axes, int resolutionIndex);


   /**
    * Get the essential metadata for the image (width, height, byte depth, rgb),
    * without retrieving pixels and full metadata
    * @param axes
    * @return
    */
   public EssentialImageMetadata getEssentialImageMetadata(HashMap<String, Object> axes,
                                                           int resolutionIndex);

}
