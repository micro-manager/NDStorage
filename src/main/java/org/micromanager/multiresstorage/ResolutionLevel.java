///////////////////////////////////////////////////////////////////////////////
//FILE:          ResolutionLevel.java
//PROJECT:       Micro-Manager
//SUBSYSTEM:     mmstudio
//-----------------------------------------------------------------------------
//
// AUTHOR:       Henry Pinkard, henry.pinkard@gmail.com, 2012
//
// COPYRIGHT:    University of California, San Francisco, 2012
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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.*;
import javax.swing.*;

import mmcorej.TaggedImage;
import mmcorej.org.json.JSONException;
import mmcorej.org.json.JSONObject;

public final class ResolutionLevel {

//   private ProgressBar savingFinishedProgressBar_;
   private JSONObject summaryMetadata_;
   private volatile boolean newDataSet_;
   private final String directory_;
   private final boolean separateMetadataFile_;
   private volatile boolean finished_ = false;
   private String summaryMetadataString_ = null;
   private int maxSliceIndex_ = 0, maxFrameIndex_ = 0, maxChannelIndex_ = 0, minSliceIndex_ = 0;
   // Images currently being written (need to keep around so that they can be
   // returned upon request via getImage()). The data structure must be
   // synchronized because the write completion is detected on a background
   // thread.
   private ConcurrentHashMap<String, TaggedImage> writePendingImages_
           = new ConcurrentHashMap<String, TaggedImage>();
   //Map of image labels to file 
   private ConcurrentHashMap<String, MultipageTiffReader> tiffReadersByLabel_;
   private static boolean showProgressBars_ = true;
   private final MultiResMultipageTiffStorage masterMultiResStorage_;
   private  int imageWidth_, imageHeight_, byteDepth_;
   private  boolean rgb_;
   private FileSet fileSet_;

   public ResolutionLevel(String dir, boolean newDataSet, JSONObject summaryMetadata,
                           MultiResMultipageTiffStorage masterMultiRes,
                          int imageWidth, int imageHeight, boolean rgb, int byteDepth) throws IOException {
      masterMultiResStorage_ = masterMultiRes;
      separateMetadataFile_ = false;

      newDataSet_ = newDataSet;
      directory_ = dir;
      tiffReadersByLabel_ = new ConcurrentHashMap<String, MultipageTiffReader>();
      setSummaryMetadata(summaryMetadata);

      if (!newDataSet_) {
         try {
            openExistingDataSet();
         } catch (Exception e) {
            JOptionPane.showMessageDialog(null, "Can't open dataset. Have you selected the top" +
                    " level folder of an NDTiffStorage dataset?");
         }
      } else {
         imageWidth_ = imageWidth;
         imageHeight_ = imageHeight;
         rgb_ = rgb;
         byteDepth_ = byteDepth;
      }

   }

   public int getNumChannels() {
      return maxChannelIndex_ + 1;
   }

   private void openExistingDataSet() {
      //Need to throw error if file not found
      MultipageTiffReader reader = null;
      File dir = new File(directory_);
      if (dir.listFiles() == null) {
         throw new RuntimeException("No files found");
      }
      int numFiles = dir.listFiles().length;

      ProgressBar progressBar = null;
      try {
         progressBar = new ProgressBar("Reading " + directory_, 0, numFiles);
      } catch (Exception e) {
         //on a system that doesnt have support for graphics
         showProgressBars_ = false;
      }
      int numRead = 0;
      if (showProgressBars_) {
         progressBar.setProgress(numRead);
         progressBar.setVisible(true);
      }
      for (File f : dir.listFiles()) {
         if (f.getName().endsWith(".tif") || f.getName().endsWith(".TIF")) {
            try {
               //this is where fixing dataset code occurs
               reader = new MultipageTiffReader(f);
               Set<String> labels = reader.getIndexKeys();
               for (String label : labels) {
                  tiffReadersByLabel_.put(label, reader);
                  maxChannelIndex_ = Math.max(maxChannelIndex_, Integer.parseInt(label.split("_")[0]));
                  maxSliceIndex_ = Math.max(maxSliceIndex_, Integer.parseInt(label.split("_")[1]));
                  minSliceIndex_ = Math.min(minSliceIndex_, Integer.parseInt(label.split("_")[1]));
                  maxFrameIndex_ = Math.max(maxFrameIndex_, Integer.parseInt(label.split("_")[2]));
               }
               if (reader.getDisplaySettings() != null) {
                  masterMultiResStorage_.setDisplaySettings(reader.getDisplaySettings());
               }
            } catch (IOException ex) {
               ex.printStackTrace();
               throw new RuntimeException("Couldn't open file: " + f.toString());
            }
         }
         numRead++;
         if (showProgressBars_) {
            progressBar.setProgress(numRead);
         }
      }
      if (showProgressBars_) {
         progressBar.setVisible(false);
      }

      if (reader != null) {
         setSummaryMetadata(reader.getSummaryMetadata(), true);
      }
      imageWidth_ = reader.getImageWidth();
         imageHeight_ = reader.getImageHeight();
         rgb_ = false;
         byteDepth_ = reader.getByteDepth();
      
      if (showProgressBars_) {
         progressBar.setProgress(1);
         progressBar.setVisible(false);
      }
   }

   public boolean hasImage(int channelIndex, int sliceIndex, int frameIndex, int positionIndex) {
      String label = channelIndex + "_" + sliceIndex + "_" + frameIndex + "_" + positionIndex;

      TaggedImage image = writePendingImages_.get(label);
      if (image != null) {
         return true;
      }

      MultipageTiffReader reader = tiffReadersByLabel_.get(label);
      if (reader != null) {
         return true;
      }
      return false;
   }

   public TaggedImage getImage(int channelIndex, int sliceIndex, int frameIndex, int positionIndex) {
      String label = channelIndex + "_" + sliceIndex + "_" + frameIndex + "_" + positionIndex;

      TaggedImage image = writePendingImages_.get(label);
      if (image != null) {
         return image;
      }

      MultipageTiffReader reader = tiffReadersByLabel_.get(label);
      if (reader == null) {
         return null;
      }
      return reader.readImage(label);
   }

   public JSONObject getImageTags(int channelIndex, int sliceIndex, int frameIndex, int positionIndex) {
      TaggedImage image = getImage(channelIndex, sliceIndex, frameIndex, positionIndex);
      if (image == null) {
         return null;
      }
      return image.tags;
   }

   /*
    * Method that allows overwrting of pixels but not MD or TIFF tags
    * so that low res stitched images can be written tile by tile
    */
   public void overwritePixels(Object pix, int channel, int slice, int frame, int position) throws IOException {
      fileSet_.overwritePixels(pix, channel, slice, frame, position);
   }

   public void putImage(TaggedImage taggedImage, String prefix,
           int tIndex, int cIndex, int zIndex, int posIndex) throws IOException {
      // Now, we must hold on to TaggedImage, so that we can return it if
      // somebody calls getImage() before the writing is finished.
      // There is a data race if the TaggedImage is modified by other code, but
      // that would be a bad thing to do anyway (will break the writer) and is
      // considered forbidden.

      String label = cIndex + "_" + zIndex + "_" + tIndex + "_" + posIndex;
      maxChannelIndex_ = Math.max(maxChannelIndex_, Integer.parseInt(label.split("_")[0]));
      writePendingImages_.put(label, taggedImage);
      if (!newDataSet_) {
         throw new RuntimeException("Tried to write image to a finished data set");
      }

      if (fileSet_ == null) {
         try {
            MultiResMultipageTiffStorage.createDir(directory_);
         } catch (Exception ex) {
            throw new RuntimeException(ex);
         }
      }

      if (fileSet_== null) {
         fileSet_ = new FileSet(prefix);
      }
      try {
         fileSet_.writeImage(taggedImage, tIndex, cIndex, zIndex, posIndex);
         tiffReadersByLabel_.put(label, fileSet_.getCurrentReader());
      } catch (IOException ex) {
         throw new RuntimeException("problem writing image to file");
      }

      writePendingImages_.remove(label);
   }

   public Set<String> imageKeys() {
      return tiffReadersByLabel_.keySet();
   }

   /**
    * Call this function when no more images are expected Finishes writing the
    * metadata file and closes it. After calling this function, the imagestorage
    * is read-only
    */
   public void finished() {
      if (finished_) {
         return;
      }
      newDataSet_ = false;
      if (fileSet_ == null) {
         // Nothing to be done.
         finished_ = true;
         return;
      }

      //Initialize progress bar on EDT
//      SwingUtilities.invokeLater(new Runnable() {
//         @Override
//         public void run() {
//            if (fileSet_ == null) {
//               //its already done
//               return;
//            }
//            savingFinishedProgressBar_ = new ProgressBar("Finishing Files", 0, 1);
//            savingFinishedProgressBar_.setProgress(0);
//            savingFinishedProgressBar_.setVisible(true);
//         }
//      });

      int count = 0;
      try {
         fileSet_.finished();
      } catch (Exception ex) {
         ex.printStackTrace();
         throw new RuntimeException(ex);
      }
      count++;
      final int currentCount = count;
//      SwingUtilities.invokeLater(new Runnable() {
//         @Override
//         public void run() {
//            if (savingFinishedProgressBar_ == null) {
//               return;
//            }
//            savingFinishedProgressBar_.setProgress(currentCount);
//         }
//      });

//      SwingUtilities.invokeLater(new Runnable() {
//         @Override
//         public void run() {
//            if (savingFinishedProgressBar_ == null) {
//               return;
//            }
//            savingFinishedProgressBar_.close();
//            savingFinishedProgressBar_ = null;
//         }
//      });

      fileSet_ = null;
      finished_ = true;
   }

   /**
    * Disposes of the tagged images in the imagestorage
    */
   public void close() {
      for (MultipageTiffReader r : new HashSet<MultipageTiffReader>(tiffReadersByLabel_.values())) {
         try {
            r.close();
         } catch (IOException ex) {
            throw new RuntimeException(ex);
         }
      }
      tiffReadersByLabel_ = null;
   }

   public boolean isFinished() {
      return !newDataSet_;
   }

   public void setSummaryMetadata(JSONObject md) {
      setSummaryMetadata(md, false);
   }

   private void setSummaryMetadata(JSONObject md, boolean showProgress) {
      summaryMetadata_ = md;
      summaryMetadataString_ = null;
      if (summaryMetadata_ != null) {
         summaryMetadataString_ = md.toString();
         ConcurrentHashMap<String, MultipageTiffReader> oldImageMap = tiffReadersByLabel_;
         tiffReadersByLabel_ = new ConcurrentHashMap<String, MultipageTiffReader>();
         if (showProgress && showProgressBars_) {
            ProgressBar progressBar = new ProgressBar("Building image location map", 0, oldImageMap.keySet().size());
            progressBar.setProgress(0);
            progressBar.setVisible(true);
            int i = 1;
            for (String label : oldImageMap.keySet()) {
               tiffReadersByLabel_.put(label, oldImageMap.get(label));
               progressBar.setProgress(i);
               i++;
            }
            progressBar.setVisible(false);
         } else {
            tiffReadersByLabel_.putAll(oldImageMap);
         }

      }
   }

   public String getSummaryMetadataString() {
      return summaryMetadataString_;
   }

   public JSONObject getSummaryMetadata() {
      return summaryMetadata_;
   }

   public String getDiskLocation() {
      return directory_;
   }

   public long getDataSetSize() {
      File dir = new File(directory_);
      LinkedList<File> list = new LinkedList<File>();
      for (File f : dir.listFiles()) {
         if (f.isDirectory()) {
            for (File fi : f.listFiles()) {
               list.add(f);
            }
         } else {
            list.add(f);
         }
      }
      long size = 0;
      for (File f : list) {
         size += f.length();
      }
      return size;
   }

   public JSONObject getDisplaySettings() {
      return masterMultiResStorage_.getDisplaySettings();
   }

   void setDisplaySettings() {
      if (fileSet_ == null) {
         //it never wrote anything
         return;
      }
      fileSet_.putDisplaySettings();
   }

   int getByteDepth() {
      return byteDepth_;
   }

   int getWidth() {
      return imageWidth_;
   }

   int getHeight() {
      return imageHeight_;
   }

   //Class encapsulating a single File (or series of files)
   private class FileSet {

      private LinkedList<MultipageTiffWriter> tiffWriters_;
      private FileWriter mdWriter_;
      private String baseFilename_;
      private String currentTiffFilename_;
      private String currentTiffUUID_;
      ;
      private String metadataFileFullPath_;
      private volatile boolean finished_ = false;
      int nextExpectedChannel_ = 0, nextExpectedSlice_ = 0, nextExpectedFrame_ = 0;
      int currentFrame_ = 0;

      public FileSet(String prefix) throws IOException {
         tiffWriters_ = new LinkedList<MultipageTiffWriter>();

         //get file path and name
         baseFilename_ = createBaseFilename(prefix);
         currentTiffFilename_ = baseFilename_ + ".tif";
         currentTiffUUID_ = "urn:uuid:" + UUID.randomUUID().toString();
         //make first writer
         tiffWriters_.add(new MultipageTiffWriter(directory_, currentTiffFilename_,
                 summaryMetadata_, masterMultiResStorage_,
                 imageWidth_, imageHeight_, rgb_, byteDepth_
         ));

         try {
            if (separateMetadataFile_) {
               startMetadataFile();
            }
         } catch (JSONException ex) {
            throw new RuntimeException("Problem with summary metadata");
         }
      }
//
//      public String getCurrentUUID() {
//         return currentTiffUUID_;
//      }

      public String getCurrentFilename() {
         return currentTiffFilename_;
      }

      public void finished() throws IOException, ExecutionException, InterruptedException {
            try {
               if (finished_) {
                  return;
               }

               if (separateMetadataFile_) {
                  try {
                     finishMetadataFile();
                  } catch (JSONException ex) {
                     throw new RuntimeException("Problem finishing metadata.txt");
                  }
               }

               //only need to finish last one here because previous ones in set are finished as they fill up with images
               tiffWriters_.getLast().finishedWriting();
               //close all
               for (MultipageTiffWriter w : tiffWriters_) {
                  w.close();
               }
               tiffWriters_ = null;
               finished_ = true;
            } catch (Exception e) {
               e.printStackTrace();
               throw new RuntimeException(e);
            }
      }

      public MultipageTiffReader getCurrentReader() {
         return tiffWriters_.getLast().getReader();
      }

      public void overwritePixels(Object pixels, int channel, int slice, int frame, int position) throws IOException {
         ArrayList<Future> list = new ArrayList<Future>();
         for (MultipageTiffWriter w : tiffWriters_) {
            if (w.getIndexMap().containsKey(StorageMD.generateLabel(channel, slice, frame, position))) {
               w.overwritePixels(pixels, channel, slice, frame, position);
            }
         }
      }

      public void writeImage(TaggedImage img, int tIndex,
           int cIndex, int zIndex, int posIndex) throws IOException {
            //check if current writer is out of space, if so, make a new one
            if (!tiffWriters_.getLast().hasSpaceToWrite(img)) {
               if (masterMultiResStorage_.debugLogger_ != null) {
                  masterMultiResStorage_.debugLogger_.accept("Creating new tiff file: tczp: " + tIndex + "  " +
                          cIndex + " " + zIndex + " " + posIndex);
               }
               try {
                  //write index map here but still need to call close() at end of acq
                  tiffWriters_.getLast().finishedWriting();
                  if (masterMultiResStorage_.debugLogger_ != null) {
                     masterMultiResStorage_.debugLogger_.accept("finished existing file");
                  }
               } catch (Exception ex) {
                  throw new RuntimeException(ex);
               }

               currentTiffFilename_ = baseFilename_ + "_" + tiffWriters_.size() + ".tif";
               currentTiffUUID_ = "urn:uuid:" + UUID.randomUUID().toString();
               try {
                  tiffWriters_.add(new MultipageTiffWriter(directory_, currentTiffFilename_,
                          summaryMetadata_, masterMultiResStorage_,
                          imageWidth_, imageHeight_, rgb_, byteDepth_
                  ));
                  if (masterMultiResStorage_.debugLogger_ != null) {
                     masterMultiResStorage_.debugLogger_.accept("Constructed new file");
                  }
               } catch (IOException e) {
                  e.printStackTrace();
                  throw new RuntimeException(e);
               }
            }

            //Add filename to image tags
            try {
               img.tags.put("FileName", currentTiffFilename_);
            } catch (JSONException ex) {
               throw new RuntimeException("Error adding filename to metadata");
            }

            //write image
            try {
//               if (masterMultiResStorage_.debugLogger_ != null) {
//                  masterMultiResStorage_.debugLogger_.accept("Writing image tczp: " + tIndex + "  " +
//                          cIndex + " " + zIndex + " " + posIndex);
//               }
               long start = System.nanoTime();
               tiffWriters_.getLast().writeImage(img, tIndex, cIndex, zIndex, posIndex);
               if (masterMultiResStorage_.debugLogger_ != null) {
//                  masterMultiResStorage_.debugLogger_.accept("Finished writing image");
                  masterMultiResStorage_.debugLogger_.accept("write_image_time_" + (System.nanoTime() - start));
               }
            } catch (IOException e) {
               e.printStackTrace();
               throw new RuntimeException(e);
            }

            try {
               if (separateMetadataFile_) {
                  writeToMetadataFile(img.tags);
               }
            } catch (JSONException ex) {
               throw new RuntimeException("Problem with image metadata");
            }
      }

      private void writeToMetadataFile(JSONObject md) throws JSONException {
//         try {
//            mdWriter_.write(",\n\"FrameKey-" + MultiresMetadata.getFrameIndex(md)
//                    + "-" + MultiresMetadata.getChannelIndex(md) + "-" + MultiresMetadata.getSliceIndex(md) + "\": ");
//            mdWriter_.write(md.toString(2));
//         } catch (IOException ex) {
//            throw new RuntimeException("Problem writing to metadata.txt file");
//         }
      }

      private void startMetadataFile() throws JSONException {
         metadataFileFullPath_ = directory_ + "/" + baseFilename_ + "_metadata.txt";
         try {
            mdWriter_ = new FileWriter(metadataFileFullPath_);
            mdWriter_.write("{" + "\n");
            mdWriter_.write("\"Summary\": ");
            mdWriter_.write(summaryMetadata_.toString(2));
         } catch (IOException ex) {
            throw new RuntimeException("Problem creating metadata.txt file");
         }
      }

      private void finishMetadataFile() throws JSONException {
         try {
            mdWriter_.write("\n}\n");
            mdWriter_.close();
         } catch (IOException ex) {
            throw new RuntimeException("Problem creating metadata.txt file");
         }
      }

      private String createBaseFilename(String prefix) {
         String baseFilename;
         if (prefix.length() == 0) {
            baseFilename = "NDTiffStack";
         } else {
            baseFilename = prefix + "_NDTiffStack";
         }

         return baseFilename;
      }

      private void putDisplaySettings() {
         tiffWriters_.getLast().setDisplayStorer();
      }
   }

   class ImageLabelComparator implements Comparator<String> {

      private final boolean slicesFirst_;
      private final boolean timeFirst_;

      public ImageLabelComparator() {
         this(false, false);
      }

      public ImageLabelComparator(boolean slicesFirst, boolean timeFirst) {
         super();
         slicesFirst_ = slicesFirst;
         timeFirst_ = timeFirst;
      }

      public boolean getSlicesFirst() {
         return slicesFirst_;
      }

      public boolean getTimeFirst() {
         return timeFirst_;
      }

      @Override
      public int compare(String s1, String s2) {
         //c_s_f_p
         String[] indices1 = s1.split("_");
         String[] indices2 = s2.split("_");
         if (timeFirst_) {
            int position1 = Integer.parseInt(indices1[3]), position2 = Integer.parseInt(indices2[3]);
            if (position1 != position2) {
               return position1 - position2;
            }
            int frame1 = Integer.parseInt(indices1[2]), frame2 = Integer.parseInt(indices2[2]);
            if (frame1 != frame2) {
               return frame1 - frame2;
            }
         } else {
            int frame1 = Integer.parseInt(indices1[2]), frame2 = Integer.parseInt(indices2[2]);
            if (frame1 != frame2) {
               return frame1 - frame2;
            }
            int position1 = Integer.parseInt(indices1[3]), position2 = Integer.parseInt(indices2[3]);
            if (position1 != position2) {
               return position1 - position2;
            }
         }
         if (slicesFirst_) {
            int channel1 = Integer.parseInt(indices1[0]), channel2 = Integer.parseInt(indices2[0]);
            if (channel1 != channel2) {
               return channel1 - channel2;
            }
            return Integer.parseInt(indices1[1]) - Integer.parseInt(indices2[1]);
         } else {
            int slice1 = Integer.parseInt(indices1[1]), slice2 = Integer.parseInt(indices2[1]);
            if (slice1 != slice2) {
               return slice1 - slice2;
            }
            return Integer.parseInt(indices1[0]) - Integer.parseInt(indices2[0]);
         }
      }
   }
}
