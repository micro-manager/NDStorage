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
package org.micromanager.ndtiffstorage;

import java.awt.*;
import java.awt.geom.Rectangle2D;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import javax.swing.*;

import mmcorej.TaggedImage;
import mmcorej.org.json.JSONObject;

public final class ResolutionLevel {

//   private ProgressBar savingFinishedProgressBar_;
   private JSONObject summaryMetadata_;
   private volatile boolean newDataSet_;
   private final String directory_;
   private volatile boolean finished_ = false;
   private String summaryMetadataString_ = null;

   //Map of image labels to file 
   private ConcurrentHashMap<String, NDTiffReader> tiffReadersByLabel_;
   private static boolean showProgressBars_ = true;
   private final NDTiffStorage masterMultiResStorage_;
   private FileSet fileSet_;
   private String prefix_;
   private ConcurrentHashMap<String, TaggedImage> writePendingImages_
           = new ConcurrentHashMap<String, TaggedImage>();
   private ConcurrentHashMap<String, EssentialImageMetadata> writePendingEssentialMetadata_
           = new ConcurrentHashMap<String, EssentialImageMetadata>();
   private IndexWriter indexWriter_;
   private int firstImageWidth_, firstImageHeight_ = 0;

   public ResolutionLevel(String dir, boolean newDataSet, JSONObject summaryMetadata,
                          NDTiffStorage masterMultiRes, String prefix) throws IOException {
      masterMultiResStorage_ = masterMultiRes;
      prefix_ = prefix;

      newDataSet_ = newDataSet;
      directory_ = dir;
      tiffReadersByLabel_ = new ConcurrentHashMap<String, NDTiffReader>();
      setSummaryMetadata(summaryMetadata);

      if (!newDataSet_) {
         try {
            openExistingDataSet();
         } catch (Exception e) {
            JOptionPane.showMessageDialog(null, "Can't open dataset. Have you selected the top" +
                    " level folder of an NDTiffStorage dataset?");
         }
      } else{
         try {
            indexWriter_ = new IndexWriter(directory_);
         } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
         }

      }
   }

   public void addToWritePendingImages(String identifier, TaggedImage ti, EssentialImageMetadata essMD) {
      writePendingImages_.put(identifier, ti);
      writePendingEssentialMetadata_.put(identifier, essMD);
   }

   public void removePendingImage(String key) {
      writePendingImages_.remove(key);
      writePendingEssentialMetadata_.remove(key);
   }


   private void openExistingDataSet() throws IOException {
      //Need to throw error if file not found
      NDTiffReader reader = null;
      File dir = new File(directory_);
      if (dir.listFiles() == null) {
         throw new RuntimeException("No files found");
      }
      TreeMap<String, IndexEntryData> indexMap = null;
      TreeMap<String, NDTiffReader> readersByFilename = new TreeMap<>();
      for (File f : dir.listFiles()) {
         if (f.getName().endsWith(".tif") || f.getName().endsWith(".TIF")) {
            try {
               //this is where fixing dataset code occurs
               reader = new NDTiffReader(f);
               readersByFilename.put(f.getName(), reader);
            } catch (IOException ex) {
               ex.printStackTrace();
               throw new RuntimeException("Couldn't open file: " + f.toString());
            }
         } else if (f.getName().endsWith("index")) {
            indexMap = IndexEntryData.readIndexMap(f);
            if (firstImageWidth_ == 0) {
               firstImageWidth_ = (int) indexMap.values().iterator().next().pixWidth_;
               firstImageHeight_ = (int) indexMap.values().iterator().next().pixHeight_;
            }
         }
      }
      //Map index keys to tiff readers
      for (String indexKey : indexMap.keySet()) {
         String filename = indexMap.get(indexKey).filename_;
         tiffReadersByLabel_.put(indexKey, readersByFilename.get(filename));
         tiffReadersByLabel_.get(indexKey).getIndexMap().put(indexKey, indexMap.get(indexKey));
      }
      summaryMetadata_ = tiffReadersByLabel_.values().iterator().next().getSummaryMetadata();
   }

   public int getFirstImageWidth() {
      return firstImageWidth_;
   }

   public int getFirstImageHeight() {
      return firstImageHeight_;
   }

   public boolean hasImage(String indexKey) {

      TaggedImage image = writePendingImages_.get(indexKey);
      if (image != null) {
         return true;
      }

      NDTiffReader reader = tiffReadersByLabel_.get(indexKey);
      if (reader != null) {
         return true;
      }
      return false;
   }

   public EssentialImageMetadata getEssentialImageMetadata(String indexKey) {
      EssentialImageMetadata md = writePendingEssentialMetadata_.get(indexKey);
      if (md != null) {
         return md;
      }

      if (tiffReadersByLabel_ == null) {
         return null;
      }
      NDTiffReader reader = tiffReadersByLabel_.get(indexKey);
      if (reader == null) {
         return null;
      }
      return reader.readEssentialImageMetadata(indexKey);
   }

   public TaggedImage getImage(String indexKey) {

      TaggedImage image = writePendingImages_.get(indexKey);
      if (image != null) {
         return image;
      }

      NDTiffReader reader = tiffReadersByLabel_.get(indexKey);
      if (reader == null) {
         return null;
      }
      return reader.readImage(indexKey);
   }

   public JSONObject getImageTags(String indexKey) {
      TaggedImage image = getImage(indexKey);
      if (image == null) {
         return null;
      }
      return image.tags;
   }

   /*
    * Method that allows overwrting of pixels but not MD or TIFF tags
    * so that low res stitched images can be written tile by tile
    */
   public void overwritePixels(String indexKey, Object pixels, boolean rgb) throws IOException {
      if (fileSet_ == null) {
       //Its hasnt been written yet
       getImage(indexKey);
      } else {
         fileSet_.overwritePixels(indexKey, pixels, rgb);
      }
   }

   public IndexEntryData putImage(String indexKey, Object pixels, byte[] metadata,
                                  boolean rgb, int imageHeight, int imageWidth, int bitDepth) throws IOException {
      if (!newDataSet_) {
         throw new RuntimeException("Tried to write image to a finished data set");
      }

      if (fileSet_ == null) {
         try {
            NDTiffStorage.createDir(directory_);
            fileSet_ = new FileSet(prefix_);
         } catch (Exception ex) {
            throw new RuntimeException(ex);
         }
      }
      try {
         IndexEntryData ied = fileSet_.writeImage(indexKey, pixels, metadata, rgb,
                 imageHeight, imageWidth, bitDepth);
         tiffReadersByLabel_.put(indexKey, fileSet_.getCurrentReader());
         if (indexWriter_ != null) {
            indexWriter_.addEntry(ied);
         }
         removePendingImage(indexKey);

         return ied;
      } catch (IOException ex) {
         throw new RuntimeException("problem writing image to file");
      }
   }

   public Set<String> imageKeys() {
      return tiffReadersByLabel_.keySet();
   }

   private void finishIndexWriter() {
      try {
         if (indexWriter_ != null) {
            indexWriter_.finishedWriting();
         }
      } catch (Exception e ) {
         throw new RuntimeException(e);
      }
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
         finishIndexWriter();
         finished_ = true;
         return;
      }

      try {
         fileSet_.finished();
      } catch (Exception ex) {
         StringWriter sw = new StringWriter();
         PrintWriter pw = new PrintWriter(sw);
         ex.printStackTrace(pw);
         String sStackTrace = sw.toString();
         if (masterMultiResStorage_.debugLogger_ != null) {
            masterMultiResStorage_.debugLogger_.accept(sStackTrace);
         }
         ex.printStackTrace();
         throw new RuntimeException(ex);
      }
      finishIndexWriter();
      fileSet_ = null;
      finished_ = true;
   }

   /**
    * Disposes of the tagged images in the imagestorage
    */
   public void close() {
      for (NDTiffReader r : new HashSet<NDTiffReader>(tiffReadersByLabel_.values())) {
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
      summaryMetadata_ = md;
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

   //Class encapsulating a single File (or series of files)
   private class FileSet {

      private LinkedList<NDTiffWriter> tiffWriters_;
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
         tiffWriters_ = new LinkedList<NDTiffWriter>();

         //get file path and name
         baseFilename_ = createBaseFilename(prefix);
         currentTiffFilename_ = baseFilename_ + ".tif";
         currentTiffUUID_ = "urn:uuid:" + UUID.randomUUID().toString();
         //make first writer
         tiffWriters_.add(new NDTiffWriter(directory_, currentTiffFilename_,
                 summaryMetadata_, masterMultiResStorage_));
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

               //only need to finish last one here because previous ones in set are finished as they fill up with images
               tiffWriters_.getLast().finishedWriting();
               if (masterMultiResStorage_.debugLogger_ != null) {
                  masterMultiResStorage_.debugLogger_.accept("Last writer finished");
               }

               tiffWriters_ = null;
               finished_ = true;
            } catch (Exception e) {
               if (masterMultiResStorage_.debugLogger_ != null) {
                  masterMultiResStorage_.debugLogger_.accept(e.getMessage());
               }
               e.printStackTrace();
               throw new RuntimeException(e);
            }
      }

      public NDTiffReader getCurrentReader() {
         return tiffWriters_.getLast().getReader();
      }

      public void overwritePixels(String identifier, Object pixels, boolean rgb) throws IOException {
         ArrayList<Future> list = new ArrayList<Future>();
         for (NDTiffWriter w : tiffWriters_) {
            if (w.getIndexMap().containsKey(identifier)) {
               w.overwritePixels(identifier, pixels, rgb);
            }
         }
      }

      public IndexEntryData writeImage(String indexKey, Object pixels, byte[] metadata,
                                       boolean rgb, int imageHeight, int imageWidth, int bitDepth) throws IOException {
            //check if current writer is out of space, if so, make a new one
            if (!tiffWriters_.getLast().hasSpaceToWrite(pixels, metadata, rgb)) {
               if (masterMultiResStorage_.debugLogger_ != null) {
                  masterMultiResStorage_.debugLogger_.accept("Creating new tiff file");
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
                  tiffWriters_.add(new NDTiffWriter(directory_, currentTiffFilename_,
                          summaryMetadata_, masterMultiResStorage_));
                  if (masterMultiResStorage_.debugLogger_ != null) {
                     masterMultiResStorage_.debugLogger_.accept("Constructed new file");
                  }
               } catch (IOException e) {
                  e.printStackTrace();
                  throw new RuntimeException(e);
               }
            }


            //write image
            try {
//               if (masterMultiResStorage_.debugLogger_ != null) {
//                  masterMultiResStorage_.debugLogger_.accept("Writing image tczp: " + tIndex + "  " +
//                          cIndex + " " + zIndex + " " + posIndex);
//               }
               long start = System.nanoTime();
               IndexEntryData indexMetedata = tiffWriters_.getLast().writeImage(indexKey, pixels, metadata,
                                  rgb, imageHeight, imageWidth, bitDepth);
               if (masterMultiResStorage_.debugLogger_ != null) {
//                  masterMultiResStorage_.debugLogger_.accept("Finished writing image");
                  masterMultiResStorage_.debugLogger_.accept("write_image_time_" + (System.nanoTime() - start));
               }
               return indexMetedata;
            } catch (IOException e) {
               e.printStackTrace();
               throw new RuntimeException(e);
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
