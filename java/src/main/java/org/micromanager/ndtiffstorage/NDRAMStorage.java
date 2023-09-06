package org.micromanager.ndtiffstorage;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.function.Consumer;
import mmcorej.TaggedImage;
import mmcorej.org.json.JSONObject;

/**
 * A class that implements the NDTiff API, but holds images in memory
 */
public class NDRAMStorage implements NDTiffAPI {

   private boolean finished_ = false;

   private HashMap<HashMap<String, Object>, Object> images_ = new HashMap<HashMap<String, Object>, Object>();
   private HashMap<HashMap<String, Object>, JSONObject> metadata_ = new HashMap<HashMap<String, Object>, JSONObject>();

   private boolean rgb_;
    private int bitDepth_;
    private int imageHeight_ = -1;
    private int imageWidth_ = -1;
    private JSONObject displaySettings_;
    private JSONObject summaryMetadata_;


    public NDRAMStorage(JSONObject summaryMetadata) {
        summaryMetadata_ = summaryMetadata;
    }

   @Override
   public Future<IndexEntryData> putImage(Object pixels, JSONObject metadata, HashMap<String, Object> axes,
                          boolean rgb, int bitDepth, int imageHeight, int imageWidth) {
      images_.put(axes, pixels);
      metadata_.put(axes, metadata);
      rgb_ = rgb;
      bitDepth_ = bitDepth;
      imageHeight_ = imageHeight;
      imageWidth_ = imageWidth;
      Future<IndexEntryData> future = CompletableFuture.completedFuture(null);
        return future;
   }

   @Override
   public boolean isFinished() {
      return finished_;
   }

   @Override
   public void checkForWritingException() throws Exception {}

   @Override
   public void setDisplaySettings(JSONObject displaySettings) {
        displaySettings_ = displaySettings;
   }

   @Override
   public JSONObject getDisplaySettings() {
      return displaySettings_;
   }

   @Override
   public JSONObject getSummaryMetadata() {
      return summaryMetadata_;
   }

   @Override
   public void finishedWriting() {
      finished_ = true;
   }

   @Override
   public void addImageWrittenListener(ImageWrittenListener iwc) {

   }

   @Override
   public String getDiskLocation() {
      return null;
   }

   @Override
   public void close() {
      images_.clear();
      metadata_.clear();
   }

   @Override
   public void closeAndWait() throws InterruptedException {
      images_.clear();
      metadata_.clear();
   }

   @Override
   public int[] getImageBounds() {
      if (imageHeight_ == -1 || imageWidth_ == -1) {
         return null; // not initialized
      }
      return new int[]{0, 0, imageWidth_, imageHeight_};
   }

   @Override
   public TaggedImage getImage(HashMap<String, Object> axes) {
      return new TaggedImage(images_.get(axes), metadata_.get(axes));
   }

   @Override
   public TaggedImage getSubImage(HashMap<String, Object> axes, int xOffset, int yOffset, int width,
                                  int height) {
      TaggedImage ti = getImage(axes);
      if (ti.pix == null) {
         return null;
      }
      if (ti.pix instanceof byte[]) {
         byte[] original = (byte[]) ti.pix;
         byte[] cropped = new byte[width * height];

         for (int y = 0; y < height; y++) {
            System.arraycopy(original, ((y + yOffset) * this.imageWidth_) + xOffset, cropped, y * width, width);
         }
         return new TaggedImage(cropped, ti.tags);
      }

      if (ti.pix instanceof short[]) {
         short[] original = (short[]) ti.pix;
         short[] cropped = new short[width * height];

         for (int y = 0; y < height; y++) {
            System.arraycopy(original, ((y + yOffset) * this.imageWidth_) + xOffset, cropped, y * width, width);
         }
         return new TaggedImage(cropped, ti.tags);
      }
      throw new UnsupportedOperationException("Unsupported pixel type");
   }

   @Override
   public EssentialImageMetadata getEssentialImageMetadata(HashMap<String, Object> axes) {
      return new EssentialImageMetadata(imageWidth_, imageHeight_, bitDepth_, rgb_);
   }

   @Override
   public boolean hasImage(HashMap<String, Object> axes) {
      return images_.containsKey(axes);
   }

   @Override
   public Set<HashMap<String, Object>> getAxesSet() {
      return images_.keySet();
   }

   @Override
   public String getUniqueAcqName() {
      return null;
   }

   @Override
   public int getWritingQueueTaskSize() {
      return 0;
   }

   @Override
   public int getWritingQueueTaskMaxSize() {
      return 0;
   }
}
