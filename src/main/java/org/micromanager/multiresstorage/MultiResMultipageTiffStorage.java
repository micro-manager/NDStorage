///////////////////////////////////////////////////////////////////////////////
// AUTHOR:       Henry Pinkard, henry.pinkard@gmail.com
//
// COPYRIGHT:    University of California, San Francisco, 2015
//
// LICENSE:      This file is distributed under the BSD license.
//               License text is included with the source distribution.
//
//               This file is distributed in the hope that it will be useful,
//               but WITHOUT ANY WARRANTY; without even the implied warranty
//               of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
//
//               IN NO EVENT SHALL THE COPYRIGHT OWNER OR
//               CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
//               INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES.
//
package org.micromanager.multiresstorage;

import java.awt.Point;
import java.io.File;
import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import mmcorej.TaggedImage;
import mmcorej.org.json.JSONException;
import mmcorej.org.json.JSONObject;

import javax.swing.*;

/**
 * This class manages multiple multipage Tiff datasets, averaging multiple 2x2
 * squares of pixels to create successively lower resolutions until the
 * downsample factor is greater or equal to the number of tiles in a given
 * direction. This condition ensures that pixels will always be divisible by the
 * downsample factor without truncation
 *
 */
public class MultiResMultipageTiffStorage implements StorageAPI, MultiresStorageAPI {

   public static final ByteOrder BYTE_ORDER = ByteOrder.nativeOrder();

   public static final String ROW_AXIS = "row";
   public static final String COL_AXIS = "column";

   private static final String FULL_RES_SUFFIX = "Full resolution";
   private static final String DOWNSAMPLE_SUFFIX = "Downsampled_x";
   private ResolutionLevel fullResStorage_;
   private TreeMap<Integer, ResolutionLevel> lowResStorages_; //map of resolution index to storage instance
   private String directory_;
   private final JSONObject summaryMD_;
   private JSONObject displaySettings_;
   private int xOverlap_, yOverlap_;
   private int fullResTileWidthIncludingOverlap_ = -1, fullResTileHeightIncludingOverlap_ = -1;
   private int tileWidth_ = -1, tileHeight_ = -1; //Indpendent of zoom level because tile sizes stay the same--which means overlap is cut off
   private volatile boolean finished_;
   private String uniqueAcqName_;
   private ExecutorService writingExecutor_;
   private volatile int maxResolutionLevel_ = 0;
   private boolean loaded_, tiled_;
   //this is how to create a concurrent set
   private Set<HashMap<String, Integer>> imageAxes_ = new ConcurrentHashMap<HashMap<String, Integer>, Boolean>().newKeySet();
   private final Integer externalMaxResLevel_;
   private String prefix_;
   Consumer<String> debugLogger_ = null;
   private LinkedBlockingQueue<Runnable> writingTaskQueue_;
   private LinkedBlockingQueue<ImageWrittenListener> imageWrittenListeners_ = new LinkedBlockingQueue<ImageWrittenListener>();

   private boolean firstImageAdded_ = false;
   //Assume that these are constant over the dataset for dipslay purposes, though they dont neccessarily need to be for storage
   private boolean rgb_;
   private int byteDepth_;

   private int writingQueueMaxSize_ = 50;

   private static final int BUFFER_DIRECT_THRESHOLD = 8192;
   private static final int BUFFER_RECYCLE_SIZE_MIN = 1024;
   private static final int BUFFER_POOL_SIZE =
           System.getProperty("sun.arch.data.model").equals("32") ? 0 : 3;
   private final ConcurrentHashMap<Integer, Deque<ByteBuffer>> pooledBuffers_;

   private volatile boolean discardData_ = false;

   /**
    * Constructor to load existing storage from disk dir --top level saving
    * directory
    */
   public MultiResMultipageTiffStorage(String dir) throws IOException {
      externalMaxResLevel_ = null;
      loaded_ = true;
      directory_ = dir;
      finished_ = true;
      pooledBuffers_ = null;
      String fullResDir = dir + (dir.endsWith(File.separator) ? "" : File.separator) + FULL_RES_SUFFIX;
      //create fullResStorage
      fullResStorage_ = new ResolutionLevel(fullResDir, false, null, this, null);
      summaryMD_ = fullResStorage_.getSummaryMetadata();
      try {
         tiled_ = StorageMD.getTiledStorage(summaryMD_);
      } catch (Exception e) {
         tiled_ = true; //Backwards compat
      }

      try {
         String path = dir + (dir.endsWith(File.separator) ? "" : File.separator) + "display_settings.txt";
         byte[] data = Files.readAllBytes(Paths.get(path));
         displaySettings_ = new JSONObject(new String(data));
      } catch (Exception e) {
         System.err.println("Couldn't read displaysettings");
      }

      imageAxes_.addAll(fullResStorage_.imageKeys().stream().map(s -> IndexEntryData.deserializeAxes(s))
              .collect(Collectors.toSet()));

      byteDepth_ = fullResStorage_.getImage(IndexEntryData.serializeAxes(
              imageAxes_.iterator().next())).pix instanceof byte[] ? 1 : 2;

      //Only valid for stitched datasets
      if (tiled_) {
         fullResTileWidthIncludingOverlap_ = StorageMD.getFullResTileWidth(summaryMD_);
         fullResTileHeightIncludingOverlap_ = StorageMD.getFullResTileWidth(summaryMD_);
         xOverlap_ = StorageMD.getPixelOverlapX(summaryMD_);
         yOverlap_ = StorageMD.getPixelOverlapY(summaryMD_);
         tileWidth_ = fullResTileWidthIncludingOverlap_ - xOverlap_;
         tileHeight_ = fullResTileHeightIncludingOverlap_ - yOverlap_;
         lowResStorages_ = new TreeMap<Integer, ResolutionLevel>();
         //create low res storages
         int resIndex = 1;
         while (true) {
            String dsDir = directory_ + (directory_.endsWith(File.separator) ? "" : File.separator)
                    + DOWNSAMPLE_SUFFIX + (int) Math.pow(2, resIndex);
            if (!new File(dsDir).exists()) {
               break;
            }
            maxResolutionLevel_ = resIndex;
            lowResStorages_.put(resIndex, new ResolutionLevel(dsDir, false,
                    null, this, null));
            resIndex++;
         }
      } else {
         //read width from the first image
         fullResTileHeightIncludingOverlap_ = fullResStorage_.getFirstImageHeight();
         fullResTileWidthIncludingOverlap_ = fullResStorage_.getFirstImageWidth();
         tileHeight_ = fullResTileHeightIncludingOverlap_;
         tileWidth_ = fullResTileWidthIncludingOverlap_;
      }
   }

   /**
    * Constructor for new storage that doesn't parse summary metadata
    */
   public MultiResMultipageTiffStorage(String dir, String name, JSONObject summaryMetadata,
           int overlapX, int overlapY, boolean tiled,
                              Integer externalMaxResLevel, int savingQueueSize,
                                       Consumer<String> debugLogger) {
      externalMaxResLevel_ = externalMaxResLevel;
      tiled_ = tiled;
      xOverlap_ = overlapX;
      yOverlap_ = overlapY;
      prefix_ = name;
      debugLogger_ = debugLogger;

      if (BUFFER_POOL_SIZE > 0) {
         pooledBuffers_ = new ConcurrentHashMap<Integer, Deque<ByteBuffer>>();
      } else {
         pooledBuffers_ = null;
      }

      loaded_ = false;
      writingExecutor_ = Executors.newSingleThreadExecutor(new ThreadFactory() {
         @Override
         public Thread newThread(Runnable r) {
            return new Thread(r, "Multipage Tiff data writing executor");
         }
      });

      writingTaskQueue_ = new LinkedBlockingQueue<Runnable>(savingQueueSize);


      try {
         //make a copy in case tag changes are needed later
         summaryMD_ = new JSONObject(summaryMetadata.toString());
         if (tiled) {
            StorageMD.setPixelOverlapX(summaryMD_, xOverlap_);
            StorageMD.setPixelOverlapY(summaryMD_, yOverlap_);
            StorageMD.setFullResTileWidth(summaryMD_, fullResTileWidthIncludingOverlap_);
            StorageMD.setFullResTileHeight(summaryMD_, fullResTileHeightIncludingOverlap_);
         }
         StorageMD.setTiledStorage(summaryMD_, tiled_);
      } catch (JSONException ex) {
         throw new RuntimeException("Couldnt copy summary metadata");
      }

      //prefix is provided by summary metadata
      try {
         uniqueAcqName_ = getUniqueAcqDirName(dir, name);
         //create acqusition directory for actual data
         directory_ = dir + (dir.endsWith(File.separator) ? "" : File.separator) + uniqueAcqName_;
      } catch (Exception e) {
         throw new RuntimeException("Couldn't make acquisition directory");
      }

      blockingWritingTaskHandoff(new Runnable() {
         @Override
         public void run() {
            //create directory for full res data
            String fullResDir = directory_ + (dir.endsWith(File.separator) ? "" : File.separator) + FULL_RES_SUFFIX;
            try {
               createDir(fullResDir);
            } catch (Exception ex) {
               throw new RuntimeException("couldn't create saving directory");
            }

            try {
               //Create full Res storage
               fullResStorage_ = new ResolutionLevel(fullResDir, true, summaryMD_,
                       MultiResMultipageTiffStorage.this, prefix_);
            } catch (IOException ex) {
               throw new RuntimeException("couldn't create Full res storage");
            }
            lowResStorages_ = new TreeMap<Integer, ResolutionLevel>();
         }
      });
   }

   static File createDir(String dir) {
      File dirrr = new File(dir);
      if (!dirrr.exists()) {
         if (!dirrr.mkdirs()) {
            JOptionPane.showMessageDialog(null, "Couldn't create directory " + dir);
            throw new RuntimeException("Unable to create directory " + dir);
         }
      }
      return dirrr;
   }

   public void setDisplaySettings(JSONObject displaySettings) {
      try {
         displaySettings_ = new JSONObject(displaySettings.toString());
      } catch (JSONException e) {
         throw new RuntimeException(e);
      }
   }

//   public static JSONObject readSummaryMetadata(String dir) throws IOException {
//      String fullResDir = dir + (dir.endsWith(File.separator) ? "" : File.separator) + FULL_RES_SUFFIX;
//      return TaggedImageStorageMultipageTiff.readSummaryMD(fullResDir);
//   }
   public String getUniqueAcqName() {
      return uniqueAcqName_ + ""; //make new instance
   }

   public int getNumResLevels() {
      return maxResolutionLevel_ + 1;
   }

   public int[] getImageBounds() {
      if (!tiled_) {
         return new int[]{0, 0, fullResTileWidthIncludingOverlap_, fullResTileHeightIncludingOverlap_};
      }
      if (tileWidth_ == -1 || tileHeight_ == -1) {
         return null;
      }
      if (!loaded_) {
         return new int[]{0, 0, getNumCols() * tileWidth_, getNumRows() * tileHeight_};
      } else {
         int yMin = (int) (getMinRow() * tileHeight_);
         int xMin = (int) (getMinCol() * tileWidth_);
         int xMax = (int) (getNumCols() * tileWidth_ + xMin);
         int yMax = (int) (getNumRows() * tileHeight_ + yMin);
         return new int[]{xMin, yMin, xMax, yMax};
      }
   }

   private int getNumRows() {
      if (imageAxes_ == null || imageAxes_.size() == 0) {
         return 1;
      }
      int maxRow = imageAxes_.stream().mapToInt(value -> value.get(ROW_AXIS)).max().getAsInt();
      int minRow = imageAxes_.stream().mapToInt(value -> value.get(ROW_AXIS)).min().getAsInt();
      return 1 + maxRow - minRow;
   }

   private int getNumCols() {
      if (imageAxes_ == null || imageAxes_.size() == 0) {
         return 1;
      }
      int maxCol = imageAxes_.stream().mapToInt(value -> value.get(COL_AXIS)).max().getAsInt();
      int minCol = imageAxes_.stream().mapToInt(value -> value.get(COL_AXIS)).min().getAsInt();
      return 1 + maxCol - minCol;
   }

   /*
    * It doesnt matter what resolution level the pixel is at since tiles
    * are the same size at every level
    */
   private long tileIndexFromPixelIndex(long i, boolean xDirection) {
      if (i >= 0) {
         return i / (xDirection ? tileWidth_ : tileHeight_);
      } else {
         //highest pixel is -1 for tile indexed -1, so need to add one to pixel values before dividing
         return (i + 1) / (xDirection ? tileWidth_ : tileHeight_) - 1;
      }
   }

   /**
    * @param dsIndex
    * @return
    */
   private boolean hasImage(int dsIndex, HashMap<String, Integer> axes) {
      ResolutionLevel storage;
      if (dsIndex == 0) {
         storage = fullResStorage_;
      } else {
         if (lowResStorages_.get(dsIndex) == null) {
            return false;
         }
         storage = lowResStorages_.get(dsIndex);
      }

      return storage.hasImage(IndexEntryData.serializeAxes(axes));
   }

//   @Override
//   public boolean hasTileByRowCol(HashMap<String, Integer> axes, int resIndex, int row, int col) {
      //      Integer posIndex;
//      if (resIndex == 0) {
//         if (posManager_ == null) {
//            posIndex = axes.containsKey(POSITION_AXIS) ? axes.get(POSITION_AXIS) :
//                    (tiled_ ? null : 0);
//         } else {
//            posIndex = posManager_.getPositionIndexFromTilePosition(resIndex, row, col);
//         }
//      } else {
//         posIndex = posManager_.getPositionIndexFromTilePosition(resIndex, row, col);
//      }
//      if (posIndex == null) {
//         return false;
//      }
//      return hasImage(resIndex, axes);
//   }

   /**
    * Return a subimage of the larger stitched image at the appropriate zoom
    * level, loading only the tiles neccesary to form the subimage
    *
    * @param axes
    * @param dsIndex 0 for full res, 1 for 2x downsample, 2 for 4x downsample,
    * etc..
    * @param x coordinate of leftmost pixel in requested resolution
    * @param y coordinate of topmost pixel in requested resolution
    * @param width pixel width of image at requested resolution
    * @param height pixel height of image at requested resolution
    * @return Tagged image or taggeded image with background pixels and null
    * tags if no pixel data is present
    */
   public TaggedImage getDisplayImage(HashMap<String, Integer> axes,
                                      int dsIndex, int x, int y, int width, int height) {
      //TODO: in theory could make byte depth independent for different images, but
      // how to know what it should be internally if only returning empty pixels
      // with no data copied in?
      if (byteDepth_ == 0) {
         return null; //Not yet initialized
      }

      Object pixels;
      if (rgb_) {
         pixels = new byte[width * height * 4];
      } else if (byteDepth_ == 1) {
         pixels = new byte[width * height];
      } else {
         pixels = new short[width * height];
      }
      //go line by line through one column of tiles at a time, then move to next column
      JSONObject topLeftMD = null;
      //first calculate how many columns and rows of tiles are relevant and the number of pixels
      //of each tile to copy into the returned image
      long previousCol = tileIndexFromPixelIndex(x, true) - 1; //make it one less than the first col in loop
      LinkedList<Integer> lineWidths = new LinkedList<Integer>();
      for (long i = x; i < x + width; i++) { //Iterate through every column of pixels in the image to be returned
         long colIndex = tileIndexFromPixelIndex(i, true);
         if (colIndex != previousCol) {
            lineWidths.add(0);
         }
         //Increment current width
         lineWidths.add(lineWidths.removeLast() + 1);
         previousCol = colIndex;
      }
      //do the same thing for rows
      long previousRow = tileIndexFromPixelIndex(y, false) - 1; //one less than first row in loop?
      LinkedList<Integer> lineHeights = new LinkedList<Integer>();
      for (long i = y; i < y + height; i++) {
         long rowIndex = tileIndexFromPixelIndex(i, false);
         if (rowIndex != previousRow) {
            lineHeights.add(0);
         }
         //add one to pixel count of current height
         lineHeights.add(lineHeights.removeLast() + 1);
         previousRow = rowIndex;
      }
      //get starting row and column
      long rowStart = tileIndexFromPixelIndex(y, false);
      long colStart = tileIndexFromPixelIndex(x, true);
      //xOffset and y offset are the distance from the top left of the display image into which 
      //we are copying data
      int xOffset = 0;
      for (long col = colStart; col < colStart + lineWidths.size(); col++) {
         int yOffset = 0;
         for (long row = rowStart; row < rowStart + lineHeights.size(); row++) {
            HashMap<String, Integer> axesCopy = IndexEntryData.deserializeAxes(IndexEntryData.serializeAxes(axes));
            //Add in axes for row and col because this is how tiles are stored
            if (tiled_) {
               axesCopy.put(ROW_AXIS, (int) row);
               axesCopy.put(COL_AXIS, (int) col);
            }
            TaggedImage tile = getImage(axesCopy, dsIndex);
            if (tile == null) {
               yOffset += lineHeights.get((int) (row - rowStart)); //increment y offset so new tiles appear in correct position
               continue; //If no data present for this tile go on to next one
            } else if ((tile.pix instanceof byte[] && ((byte[]) tile.pix).length == 0)
                    || (tile.pix instanceof short[] && ((short[]) tile.pix).length == 0)) {
               //Somtimes an inability to read IFDs soon after they are written results in an image being read 
               //with 0 length pixels. Can't figure out why this happens, but it is rare and will result at worst with
               //a black flickering during acquisition
               yOffset += lineHeights.get((int) (row - rowStart)); //increment y offset so new tiles appear in correct position
               continue;
            }
            //take top left tile for metadata
            if (topLeftMD == null) {
               topLeftMD = tile.tags;
            }
            //Copy pixels into the image to be returned
            //yOffset is how many rows from top of viewable area, y is top of image to top of area
            for (int line = yOffset; line < lineHeights.get((int) (row - rowStart)) + yOffset; line++) {
               int tileYPix = (int) ((y + line) % tileHeight_);
               int tileXPix = (int) ((x + xOffset) % tileWidth_);
               //make sure tile pixels are positive
               while (tileXPix < 0) {
                  tileXPix += tileWidth_;
               }
               while (tileYPix < 0) {
                  tileYPix += tileHeight_;
               }
               try {
                  int multiplier = rgb_ ? 4 : 1;
                  if (dsIndex == 0) {
                     //account for overlaps when viewing full resolution tiles
                     tileYPix += yOverlap_ / 2;
                     tileXPix += xOverlap_ / 2;
                     System.arraycopy(tile.pix, multiplier * (tileYPix
                             * fullResTileWidthIncludingOverlap_ + tileXPix),
                             pixels, (xOffset + width * line) * multiplier,
                             multiplier * lineWidths.get((int) (col - colStart)));
                  } else {
                     System.arraycopy(tile.pix, multiplier * (tileYPix * tileWidth_
                             + tileXPix), pixels, multiplier * (xOffset + width * line),
                             multiplier * lineWidths.get((int) (col - colStart)));
                  }
               } catch (Exception e) {
                  e.printStackTrace();
                  throw new RuntimeException("Problem copying pixels");
               }
            }
            yOffset += lineHeights.get((int) (row - rowStart));

         }
         xOffset += lineWidths.get((int) (col - colStart));
      }

      return new TaggedImage(pixels, topLeftMD);
   }

//   /**
//    * Called before any images have been added to initialize the resolution to
//    * the specifiec zoom level
//    *
//    * @param resIndex
//    */
//   public void initializeToLevel(int resIndex) {
//      //create a null pointer in lower res storages to signal addToLoResStorage function
//      //to continue downsampling to this level
//      maxResolutionLevel_ = resIndex;
//      //Make sure position nodes for lower resolutions are created if they weren't automatically
//      posManager_.updateLowerResolutionNodes(resIndex);
//   }
   /**
    * create an additional lower resolution levels for zooming purposes
    */
   private void addResolutionsUpTo(int index) throws InterruptedException, ExecutionException {
         if (index <= maxResolutionLevel_) {
            return;
         }
         int oldLevel = maxResolutionLevel_;
         maxResolutionLevel_ = index;
         for (int i = oldLevel + 1; i <= maxResolutionLevel_; i++) {
            populateNewResolutionLevel(i);
         }
   }

   private void downsample(Object currentLevelPix, Object previousLevelPix, int fullResRow,
                           int fullResCol, int resolutionIndex,
                           boolean rgb) {
      int byteDepth = currentLevelPix instanceof byte[] ? 1: 2;
      //Determine which position in 2x2 this tile sits in
      int resAboveCol = (int) Math.floor(fullResCol / Math.pow(2, resolutionIndex - 1));
      int resAboveRow = (int) Math.floor(fullResRow / Math.pow(2, resolutionIndex - 1));
      int xPos = (int) Math.abs(resAboveCol % 2);
      int yPos = (int) Math.abs(resAboveRow % 2);
      //Add one if top or left so border pixels from an odd length image gets added in
      for (int x = 0; x < tileWidth_; x += 2) { //iterate over previous res level pixels
         for (int y = 0; y < tileHeight_; y += 2) {
            //average a square of 4 pixels from previous level
            //edges: if odd number of pixels in tile, round to determine which
            //tiles pixels make it to next res level

            //these are the indices of pixels at the previous res level, which are offset
            //when moving from res level 0 to one as we throw away the overlapped image edges
            int pixelX, pixelY, previousLevelWidth, previousLevelHeight;
            if (resolutionIndex == 1) {
               //add offsets to account for overlap pixels at resolution level 0
               pixelX = x + xOverlap_ / 2;
               pixelY = y + yOverlap_ / 2;
               previousLevelWidth = fullResTileWidthIncludingOverlap_;
               previousLevelHeight = fullResTileHeightIncludingOverlap_;
            } else {
               pixelX = x;
               pixelY = y;
               previousLevelWidth = tileWidth_;
               previousLevelHeight = tileHeight_;

            }
            int rgbMultiplier_ = rgb ? 4 : 1;
            for (int compIndex = 0; compIndex < (rgb ? 4 : 1); compIndex++) {
               int count = 1; //count is number of pixels (out of 4) used to create a pixel at this level
               //always take top left pixel, maybe take others depending on whether at image edge
               int sum = 0;
               if (rgb) {
                  sum += ((byte[]) previousLevelPix)[(pixelY * previousLevelWidth + pixelX) * rgbMultiplier_ + compIndex] & 0xff;
               } else if (byteDepth == 1) {
                  sum += ((byte[]) previousLevelPix)[(pixelY * previousLevelWidth + pixelX)] & 0xff;
               } else {
                  sum += ((short[]) previousLevelPix)[(pixelY * previousLevelWidth + pixelX)] & 0xffff;
               }

               //pixel index can be different from index in tile at resolution level 0 if there is nonzero overlap
               if (x < previousLevelWidth - 1 && y < previousLevelHeight - 1) { //if not bottom right corner, add three more pix
                  count += 3;
                  if (rgb) {
                     sum += (((byte[]) previousLevelPix)[((pixelY + 1) * previousLevelWidth + pixelX + 1) * rgbMultiplier_ + compIndex] & 0xff)
                             + (((byte[]) previousLevelPix)[(pixelY * previousLevelWidth + pixelX + 1) * rgbMultiplier_ + compIndex] & 0xff)
                             + (((byte[]) previousLevelPix)[((pixelY + 1) * previousLevelWidth + pixelX) * rgbMultiplier_ + compIndex] & 0xff);
                  } else if (byteDepth == 1) {
                     sum += (((byte[]) previousLevelPix)[((pixelY + 1) * previousLevelWidth + pixelX + 1)  ] & 0xff)
                             + (((byte[]) previousLevelPix)[(pixelY * previousLevelWidth + pixelX + 1)  ] & 0xff)
                             + (((byte[]) previousLevelPix)[((pixelY + 1) * previousLevelWidth + pixelX)  ] & 0xff);
                  } else {
                     sum += (((short[]) previousLevelPix)[((pixelY + 1) * previousLevelWidth + pixelX + 1)  ] & 0xffff)
                             + (((short[]) previousLevelPix)[(pixelY * previousLevelWidth + pixelX + 1)  ] & 0xffff)
                             + (((short[]) previousLevelPix)[((pixelY + 1) * previousLevelWidth + pixelX)  ] & 0xffff);
                  }
               } else if (x < previousLevelWidth - 1) { //if not right edge, add one more pix
                  count++;
                  if (rgb) {
                     sum += ((byte[]) previousLevelPix)[(pixelY * previousLevelWidth + pixelX + 1) * rgbMultiplier_ + compIndex] & 0xff;
                  } else if (byteDepth == 1 ) {
                     sum += ((byte[]) previousLevelPix)[(pixelY * previousLevelWidth + pixelX + 1)  ] & 0xff;
                  } else {
                     sum += ((short[]) previousLevelPix)[(pixelY * previousLevelWidth + pixelX + 1)  ] & 0xffff;
                  }
               } else if (y < previousLevelHeight - 1) { // if not bottom edge, add one more pix
                  count++;
                  if (rgb) {
                     sum += ((byte[]) previousLevelPix)[((pixelY + 1) * previousLevelWidth + pixelX) * rgbMultiplier_ + compIndex] & 0xff;
                  } else if (byteDepth == 1 ) {
                     sum += ((byte[]) previousLevelPix)[((pixelY + 1) * previousLevelWidth + pixelX) ] & 0xff;
                  } else {
                     sum += ((short[]) previousLevelPix)[((pixelY + 1) * previousLevelWidth + pixelX) ] & 0xffff;
                  }
               } else {
                  //it is the bottom right corner, no more pix to add
               }
               //add averaged pixel into appropriate quadrant of current res level
               //if full res tile has an odd number of pix, the last one gets chopped off
               //to make it fit into tile containers
               try {
                  int index = (((y + yPos * tileHeight_) / 2) * tileWidth_ + (x + xPos * tileWidth_) / 2)
                          * (rgb ? rgbMultiplier_ : 1) + compIndex;
                  if (byteDepth == 1 || rgb) {
                     ((byte[]) currentLevelPix)[index] = (byte) Math.round(sum / count);
                  } else {
                     ((short[]) currentLevelPix)[index] = (short) Math.round(sum / count);
                  }
               } catch (Exception e) {
                  e.printStackTrace();
                  throw new RuntimeException("Couldn't copy pixels to lower resolution");
               }

            }
         }
      }
   }

   private void populateNewResolutionLevel(int resolutionIndex) {
      createDownsampledStorage(resolutionIndex);
      //add all tiles from existing resolution levels to this new one
      ResolutionLevel previousLevelStorage
              = resolutionIndex == 1 ? fullResStorage_ : lowResStorages_.get(resolutionIndex - 1);
      Set<String> imageKeys = previousLevelStorage.imageKeys();
      for (String key : imageKeys) {
         HashMap<String, Integer> axes = IndexEntryData.deserializeAxes(key);
         int fullResCol = axes.get(COL_AXIS);
         int fullResRow = axes.get(ROW_AXIS);

//         int fullResPosIndex = posManager_.getFullResPositionIndex(, resolutionIndex - 1);
//         int rowIndex = (int) posManager_.getGridRow(fullResPosIndex, resolutionIndex);
//         int colIndex = (int) posManager_.getGridCol(fullResPosIndex, resolutionIndex);
         TaggedImage ti = previousLevelStorage.getImage(key);
         addToLowResStorage(ti, axes,resolutionIndex - 1, fullResRow, fullResCol, rgb_);
      }
   }

   /**
    * return a future for when the current res level is done writing
    */
   private void addToLowResStorage(TaggedImage img, HashMap<String, Integer> axes,
                                     int previousResIndex, int fullResRow, int fullResCol,
                                     boolean rgb) {
         //Read indices
         Object previousLevelPix = img.pix;
         int resolutionIndex = previousResIndex + 1;

      while (resolutionIndex <= maxResolutionLevel_) {
            //Create this storage level if needed and add all existing tiles form the previous one
            if (!lowResStorages_.containsKey(resolutionIndex)) {
               //re add all tiles from previous res level
               populateNewResolutionLevel(resolutionIndex);
            }

            //copy and change and row and col to reflect lower resolution
            HashMap<String, Integer> axesCopy = IndexEntryData.deserializeAxes(IndexEntryData.serializeAxes(axes));
            axesCopy.put(COL_AXIS, (int) Math.floor(fullResCol / Math.pow(2, resolutionIndex)));
            axesCopy.put(ROW_AXIS, (int) Math.floor(fullResRow / Math.pow(2, resolutionIndex)));

            //Create pixels or get appropriate pixels to add to
            TaggedImage existingImage = lowResStorages_.get(resolutionIndex).getImage(IndexEntryData.serializeAxes(axesCopy));

            Object currentLevelPix;
            if (existingImage == null) {
               if (rgb) {
                  currentLevelPix = new byte[tileWidth_ * tileHeight_ * 4];
               } else if (img.pix instanceof byte[]) {
                  currentLevelPix = new byte[tileWidth_ * tileHeight_];
               } else {
                  currentLevelPix = new short[tileWidth_ * tileHeight_];
               }
            } else {
               currentLevelPix = existingImage.pix;
            }

            downsample(currentLevelPix, previousLevelPix, fullResRow, fullResCol, resolutionIndex, rgb);

            //store this tile in the storage class correspondign to this resolution
            try {

               String indexKey = IndexEntryData.serializeAxes(axesCopy);
               if (existingImage == null) {     //Image doesn't yet exist at this level, so add it

                  //create a copy of tags so tags from a different res level arent inadverntanly modified
                  // while waiting for being written to disk
                  JSONObject tags = new JSONObject(img.tags.toString());
                  //modify tags to reflect image size, and correct position index
//               MultiresMetadata.setWidth(tags, tileWidth_);
//               MultiresMetadata.setHeight(tags, tileHeight_);

                  IndexEntryData ied = lowResStorages_.get(resolutionIndex).putImage(indexKey, currentLevelPix, tags.toString().getBytes(),
                          rgb, tileHeight_, tileWidth_);
                  for (ImageWrittenListener l : imageWrittenListeners_) {
                     l.imageWritten(ied);
                  }

               } else {
                  //Image already exists, only overwrite pixels to include new tiles
                  lowResStorages_.get(resolutionIndex).overwritePixels(indexKey, currentLevelPix, rgb);
               }

            } catch (Exception e) {
               e.printStackTrace();
               throw new RuntimeException("Couldnt modify tags for lower resolution level");
            }

            //go on to next level of downsampling
            previousLevelPix = currentLevelPix;
            resolutionIndex++;
         }
   }

   private void createDownsampledStorage(int resIndex) {
      String dsDir = directory_ + (directory_.endsWith(File.separator) ? "" : File.separator)
              + DOWNSAMPLE_SUFFIX + (int) Math.pow(2, resIndex);
      try {
         createDir(dsDir);
      } catch (Exception ex) {
         throw new RuntimeException("copuldnt create directory");
      }
      try {
         JSONObject smd = new JSONObject(summaryMD_.toString());
         ResolutionLevel storage = new ResolutionLevel(dsDir, true, smd, this, prefix_);
         lowResStorages_.put(resIndex, storage);
      } catch (Exception ex) {
         throw new RuntimeException("Couldnt create downsampled storage");
      }
   }

   public void discardDataForDebugging() {
      discardData_ = true;
   }



   /**
    * This version works for regular, non-multiresolution data
    *
    * @param ti
    * @param axessss
    */
   public Future putImage(TaggedImage ti, HashMap<String, Integer> axessss,
                        boolean rgb, int imageHeight, int imageWidth) {
//      try {
//         Thread.sleep(20);
//      } catch (Exception e) {
//
//      }

      if (!firstImageAdded_) {
         firstImageAdded_ = true;
         fullResTileWidthIncludingOverlap_ = imageWidth;
         fullResTileHeightIncludingOverlap_ = imageHeight;
         tileWidth_ = fullResTileWidthIncludingOverlap_ - xOverlap_;
         tileHeight_ = fullResTileHeightIncludingOverlap_ - yOverlap_;
      }

      rgb_ = rgb;
      byteDepth_ = (ti.pix instanceof byte[] ? 1 : 2);

      if (debugLogger_ != null) {
         debugLogger_.accept(
                 "Adding image " + getAxesString(axessss) +
                 "\nwriting_queue_size= " + writingTaskQueue_.size());
      }
      while (writingTaskQueue_.size() > writingQueueMaxSize_) {
         try {
            Thread.sleep(1);
         } catch (InterruptedException e) {
            throw new RuntimeException(e);
         }
      }

      Object pixels = ti.pix;
      byte[] metadata = ti.tags.toString().getBytes();
      String indexKey = IndexEntryData.serializeAxes(axessss);
      fullResStorage_.addToWritePendingImages(indexKey, ti);

   return blockingWritingTaskHandoff(new Runnable() {
      @Override
      public void run() {
         if (discardData_) {
            return;
         }
         try {
            //Make a local copy
            HashMap<String, Integer> axes = new HashMap<String, Integer>(axessss);
            imageAxes_.add(axes);

            //write to full res storage as normal (i.e. with overlap pixels present)
            IndexEntryData ied = fullResStorage_.putImage(indexKey, pixels, metadata,
                    rgb, imageHeight, imageWidth);
            for (ImageWrittenListener l : imageWrittenListeners_) {
               l.imageWritten(ied);
            }

         } catch (IOException ex) {
            throw new RuntimeException(ex.toString());
         }
      }
   });
   }

   static String getAxesString(HashMap<String, Integer> axes) {
      String s = "";
      for (String key : axes.keySet()) {
         s += key + "  " + axes.get(key) + ",  ";
      }
      return s;
   }

   /**
    * This version is called by programs doing dynamic stitching (i.e.
    * micro-magellan). axes must contain "position" mapping to a desired position index
    *
    * @return
    */
   @Override
   public Future putImageMultiRes( TaggedImage ti, final HashMap<String, Integer> axes,
                                  boolean rgb, int imageHeight, int imageWidth) {
      if (!firstImageAdded_) {
         //technically this doesnt need to be parsed here, because it should be fixed for the whole
         //dataset, NOT interpretted at runtime, but whatever
         firstImageAdded_ = true;
         fullResTileWidthIncludingOverlap_ = imageWidth;
         fullResTileHeightIncludingOverlap_ = imageHeight;
         tileWidth_ = fullResTileWidthIncludingOverlap_ - xOverlap_;
         tileHeight_ = fullResTileHeightIncludingOverlap_ - yOverlap_;
      }

      rgb_ = rgb;
      byteDepth_ = (ti.pix instanceof byte[] ? 1 : 2);

      Object pixels = ti.pix;
      byte[] metadata = ti.tags.toString().getBytes();
      String indexKey = IndexEntryData.serializeAxes(StorageMD.getAxes(ti.tags));
      fullResStorage_.addToWritePendingImages(indexKey, ti);
      return blockingWritingTaskHandoff(new Runnable() {
         @Override
         public void run() {
            try {
               if (tiled_) {
                  if (!axes.containsKey(ROW_AXIS) || !axes.containsKey(COL_AXIS)) {
                     throw new RuntimeException("axes must contain row and column infor");
                  }
               }
               imageAxes_.add(axes);

               //write to full res storage as normal (i.e. with overlap pixels present)
               IndexEntryData ied = fullResStorage_.putImage(indexKey, pixels, metadata, rgb, imageHeight, imageWidth);
               for (ImageWrittenListener l : imageWrittenListeners_) {
                  l.imageWritten(ied);
               }

               if (tiled_) {
                  //check if maximum resolution level needs to be updated based on full size of image
                  long fullResPixelWidth = getNumCols() * tileWidth_;
                  long fullResPixelHeight = getNumRows() * tileHeight_;
                  int maxResIndex = externalMaxResLevel_ != null ? externalMaxResLevel_ :
                          (int) Math.ceil(Math.log((Math.max(fullResPixelWidth, fullResPixelHeight)
                                  / 4)) / Math.log(2));
                  int row = axes.get(ROW_AXIS);
                  int col = axes.get(COL_AXIS);
                  addResolutionsUpTo(maxResIndex);
                  addToLowResStorage(ti, axes, 0, row, col, rgb );
               }

            } catch (IOException | ExecutionException | InterruptedException ex) {
               throw new RuntimeException(ex.toString());
            }
         }
      });
   }

   public boolean hasImage(HashMap<String, Integer> axes, int downsampleIndex) {
      if (downsampleIndex == 0) {
         return fullResStorage_.hasImage(IndexEntryData.serializeAxes(axes));
      } else {
         return lowResStorages_.containsKey(downsampleIndex) && lowResStorages_.get(downsampleIndex)
                 .hasImage(IndexEntryData.serializeAxes(axes));
      }
   }

   @Override
   public TaggedImage getImage(HashMap<String, Integer> axes) {
      //full resolution
      return getImage(axes, 0);
   }

   @Override
   public TaggedImage getImage(HashMap<String, Integer> axes, int dsIndex) {
      //return a single tile from the full res image
      if (dsIndex == 0) {
         return fullResStorage_.getImage(IndexEntryData.serializeAxes(axes));
      } else {
         return lowResStorages_.get(dsIndex).getImage(IndexEntryData.serializeAxes(axes));
      }
   }

   @Override
   public void addImageWrittenListener(ImageWrittenListener iwl) {
      imageWrittenListeners_.add(iwl);
   }

   private Future blockingWritingTaskHandoff(Runnable r) {
      //Wait if queue is full, otherwise add and signal to running executor do it
      try {
         writingTaskQueue_.put(r);
         return writingExecutor_.submit(new Runnable() {
            @Override
            public void run() {
               try {
                  writingTaskQueue_.take().run();
               } catch (InterruptedException e) {
                  throw new RuntimeException(e);
               }
            }
         });

      } catch (InterruptedException e) {
         throw new RuntimeException(e);
      }
   }

   /**
    * Singal to finish writing and block until everything pending is done
    */
   public void finishedWriting()  {
      if (debugLogger_ != null) {
         debugLogger_.accept("Finished writing. Remaining writing task queue size = " + writingTaskQueue_.size());
      }
      blockingWritingTaskHandoff(new Runnable() {
         @Override
         public void run() {
            if (debugLogger_ != null) {
               debugLogger_.accept("Finishing writing executor");
            }
            if (finished_) {
               return;
            }
            if (debugLogger_ != null) {
               debugLogger_.accept("Finishing fullres storage");
            }
            fullResStorage_.finished();
            if (tiled_) {
               for (ResolutionLevel s : lowResStorages_.values()) {
                  if (s != null) {
                     //s shouldn't be null ever, this check is to prevent window from getting into unclosable state
                     //when other bugs prevent storage from being properly created
                     s.finished();
                  }
               }
            }
            if (debugLogger_ != null) {
               debugLogger_.accept("Shutting down writing excutor");
            }
            writingExecutor_.shutdown();
            if (displaySettings_ != null) {
               DisplaySettingsWriter w = new DisplaySettingsWriter(directory_);
               try {
                  w.add(displaySettings_);
               } catch (IOException e) {
                  e.printStackTrace();
               }
               w.finishedWriting();
            }
            for (ImageWrittenListener l : imageWrittenListeners_) {
               l.imageWritten(IndexEntryData.createFinishedEntry());
            }
            for (ImageWrittenListener l : imageWrittenListeners_) {
               l.awaitCompletion();
            }
            imageWrittenListeners_ = null;
            if (debugLogger_ != null) {
               debugLogger_.accept("Display settings written");
            }
            
         }
      });
      if (debugLogger_ != null) {
         debugLogger_.accept("Awaiting writing executor termination");
      }
      while (true) {
         try {
            if (writingExecutor_.awaitTermination(10, TimeUnit.MILLISECONDS)) {
               break;
            }
         } catch (InterruptedException e) {
            finished_ = true;
            throw new RuntimeException(e);
         }
      }
      finished_ = true;
      if (debugLogger_ != null) {
         debugLogger_.accept("Writing executor complete");
      }
   }

   public boolean isFinished() {
      return finished_;
   }

   public JSONObject getSummaryMetadata() {
      return fullResStorage_.getSummaryMetadata();
   }

   public JSONObject getDisplaySettings() {
      return displaySettings_;
   }

   public void close() {
      //put closing on differnt channel so as to not hang up EDT while waiting for finishing
      //but cant put on writing executor because thats shutdown
      new Thread(new Runnable() {
         @Override
         public void run() {
            if (!loaded_) {
               while (true) {
                  try {
                     if (writingExecutor_.awaitTermination(10, TimeUnit.MILLISECONDS)) break;
                  } catch (InterruptedException e) {
                     throw new RuntimeException(e);
                  }
               }
            }
            fullResStorage_.close();
            for (ResolutionLevel s : lowResStorages_.values()) {
               if (s != null) { //this only happens if the viewer requested new resolution levels that were never filled in because no iamges arrived
                  s.close();
               }
            }
         }
      }).start();
   }

   public String getDiskLocation() {
      //For display purposes
      return directory_;
   }

   public long getDataSetSize() {
      long sum = 0;
      sum += fullResStorage_.getDataSetSize();
      for (ResolutionLevel s : lowResStorages_.values()) {
         sum += s.getDataSetSize();
      }
      return sum;
   }

   //Copied from MMAcquisition
   private String getUniqueAcqDirName(String root, String prefix) throws Exception {
      File rootDir = createDir(root);
      int curIndex = getCurrentMaxDirIndex(rootDir, prefix + "_");
      return prefix + "_" + (1 + curIndex);
   }

   private static int getCurrentMaxDirIndex(File rootDir, String prefix) throws NumberFormatException {
      int maxNumber = 0;
      int number;
      String theName;
      for (File acqDir : rootDir.listFiles()) {
         theName = acqDir.getName();
         if (theName.toUpperCase().startsWith(prefix.toUpperCase())) {
            try {
               //e.g.: "blah_32.ome.tiff"
               Pattern p = Pattern.compile("\\Q" + prefix.toUpperCase() + "\\E" + "(\\d+).*+");
               Matcher m = p.matcher(theName.toUpperCase());
               if (m.matches()) {
                  number = Integer.parseInt(m.group(1));
                  if (number >= maxNumber) {
                     maxNumber = number;
                  }
               }
            } catch (NumberFormatException e) {
            } // Do nothing.
         }
      }
      return maxNumber;
   }

   /**
    *
    * @return set of points (col, row) with indices of tiles that have been
    * added at this slice index
    */
   public Set<Point> getTileIndicesWithDataAt(int sliceIndex) {
      Set<Point> exploredTiles = new TreeSet<Point>(new Comparator<Point>() {
         @Override
         public int compare(Point o1, Point o2) {
            if (o1.x != o2.x) {
               return o1.x - o2.x;
            } else if (o1.y != o2.y) {
               return o1.y - o2.y;
            }
            return 0;
         }
      });
      for (HashMap<String, Integer> s : imageAxes_) {
         if (s.get("z") == sliceIndex) {
            exploredTiles.add(new Point( s.get(COL_AXIS), s.get(ROW_AXIS)));
         }

      }
      return exploredTiles;
   }

   private long getMinRow() {
      if (imageAxes_ == null || imageAxes_.size() == 0) {
         return 0;
      }
      return imageAxes_.stream().mapToInt(value -> value.get(ROW_AXIS)).min().getAsInt();
   }

   private long getMinCol() {
      if (imageAxes_ == null || imageAxes_.size() == 0) {
         return 0;
      }
      return imageAxes_.stream().mapToInt(value -> value.get(COL_AXIS)).min().getAsInt();
   }

   @Override
   public Set<HashMap<String, Integer>> getAxesSet() {
      return imageAxes_;
   }


   //
   // Buffer allocation and recycling
   //

   // The idea here is to recycle the direct buffers for image pixels, because
   // allocation is slow. We do not need a large pool,
   // because the only aim is to avoid situations where allocation is limiting
   // at steady state. If writing is, on average, faster than incoming images,
   // the pool should always have a buffer ready for a new request.
   // Ideally we would also evict unused buffers after a timeout, so as not to
   // leak memory after writing has concluded.


   private static Buffer allocateByteBuffer(int capacity) {
//      Buffer b = capacity >= BUFFER_DIRECT_THRESHOLD ?
//              ByteBuffer.allocateDirect(capacity) :
//              ByteBuffer.allocate(capacity);
      Buffer b =  ByteBuffer.allocateDirect(capacity);
      b = ((ByteBuffer) b).order(BYTE_ORDER);
      return b;
   }

   Buffer getSmallBuffer(int capacity) {
      return allocateByteBuffer(capacity);
   }

   Buffer getLargeBuffer(int capacity) {
      if (capacity < BUFFER_RECYCLE_SIZE_MIN) {
         return allocateByteBuffer(capacity);
      }
      if (BUFFER_POOL_SIZE == 0) {
         Buffer b =  allocateByteBuffer(capacity);
         return b;
      }

      if (!pooledBuffers_.containsKey(capacity)) {
         pooledBuffers_.put(capacity, new ArrayDeque<>(BUFFER_POOL_SIZE));
      }

      // Recycle in LIFO order (smaller images may still be in L3 cache)
      Buffer b = pooledBuffers_.get(capacity).pollFirst();

      if (b != null) {
         // Ensure correct byte order in case recycled from other source
         ((ByteBuffer)b).order(BYTE_ORDER);
         //You can't chain the previous and following calls together or you get a weird java error
         b.clear();
         return b;
      }
      return allocateByteBuffer(capacity);
   }

   void tryRecycleLargeBuffer(ByteBuffer b) {
      // Keep up to BUFFER_POOL_SIZE direct buffers of the current size
      if (BUFFER_POOL_SIZE == 0 || !b.isDirect()) {
         return;
      }
      if (!pooledBuffers_.containsKey(b.capacity())) {
         pooledBuffers_.put(b.capacity(), new ArrayDeque<>(BUFFER_POOL_SIZE));
      }

      if (pooledBuffers_.get(b.capacity()).size() == BUFFER_POOL_SIZE) {
         pooledBuffers_.get(b.capacity()).removeLast(); // Discard oldest
      }
      pooledBuffers_.get(b.capacity()).addFirst(b);
   }

}
