package org.micromanager.multiresstorage;

import mmcorej.org.json.JSONException;
import mmcorej.org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.function.Consumer;

/**
 * Wrapper for data stored in file index for fast access
 * Each instance corresponds to a single image + metadata
 */
public class IndexEntryData {


   public static final int EIGHT_BIT = 0;
   public static final int SIXTEEN_BIT = 1;
   public static final int EIGHT_BIT_RGB = 2;

   public static final int UNCOMPRESSED = 0;

   public final String axesKey_;
   public final long pixOffset_;
   public final long pixWidth_, pixHeight_;
   public final long pixelType_;
   public final long pixelCompression_;
   public final long mdOffset_;
   public final long mdLength_;
   public final long mdCompression_;
   public final String filename_;

   public final boolean dataSetFinishedEntry_;

   public IndexEntryData(String axesKey, long pixelType, long pixOffset,
                         long pixWidth, long pixHeight, long mdOffset, long mdLength,
                         String filename) {
      axesKey_ = axesKey;
      pixOffset_ = pixOffset;
      pixWidth_ = pixWidth;
      pixHeight_ = pixHeight;
      mdLength_ = mdLength;
      mdOffset_ = mdOffset;
      pixelType_ = pixelType;
      pixelCompression_ = UNCOMPRESSED;
      mdCompression_ = UNCOMPRESSED;
      filename_ = filename;
      dataSetFinishedEntry_ = false;
   }

   public static IndexEntryData createFinishedEntry() {
      return new IndexEntryData();
   }

   /**
    * Used when finished writing to signal end of dataset
    */
   public IndexEntryData() {
      axesKey_ = null;
      pixOffset_ = 0;
      pixWidth_ = 0;
      pixHeight_ = 0;
      mdLength_ = 0;
      mdOffset_ = 0;
      pixelType_ = 0;
      pixelCompression_ = UNCOMPRESSED;
      mdCompression_ = UNCOMPRESSED;
      filename_ = null;
      dataSetFinishedEntry_ = true;
   }

   public boolean isDataSetFinishedEntry() {
      return dataSetFinishedEntry_;
   }

   private static long unsignInt(int i) {
      long val = Integer.MAX_VALUE & i;
      if (i < 0) {
         final long BIGGEST_INT_BIT = (long) Math.pow(2, 31);
         val += BIGGEST_INT_BIT;
      }
      return val;
   }

   public static TreeMap<String, IndexEntryData> readIndexMap(File f) throws IOException {
      RandomAccessFile rf = new RandomAccessFile(f, "rw");
      FileChannel channel = rf.getChannel();
      ByteBuffer buffer = ByteBuffer.allocate((int) rf.length()).order(ByteOrder.nativeOrder());
      channel.read(buffer);
      buffer.rewind();
      TreeMap<String, IndexEntryData> indexMap = new TreeMap<>();
      while (buffer.position() < rf.length()) {
         long axesLength = buffer.getInt();
         byte[] axesBuffer = new byte[(int) axesLength];
         buffer.get(axesBuffer);
         String axesString = new String(axesBuffer);
         HashMap<String, Integer> axes = IndexEntryData.deserializeAxes(axesString);
         long filenameLength = buffer.getInt();
         byte[] filenameBuffer = new byte[(int)filenameLength];
         buffer.get(filenameBuffer);
         String filename = new String(filenameBuffer);
         long pixelOffset = unsignInt(buffer.getInt());
         long pixelWidth = buffer.getInt();
         long pixelHeight = buffer.getInt();
         long pixelType = buffer.getInt();
         long pixelCompression = buffer.getInt();
         long metadataOffset = unsignInt(buffer.getInt());
         long metadataLength = buffer.getInt();
         long metadataCompression = buffer.getInt();
         String indexKey = serializeAxes(axes);
         IndexEntryData ide = new IndexEntryData(indexKey, pixelType,
                 pixelOffset, pixelWidth, pixelHeight,
                 metadataOffset, metadataLength, filename);
         indexMap.put(indexKey, ide);
      }
      channel.close();
      rf.close();
      return indexMap;
   }

   public boolean isRGB() {
      return pixelType_ == EIGHT_BIT_RGB;
   }

   public int getByteDepth() {
      return pixelType_ == SIXTEEN_BIT ? 2 : 1;
   }

   /**
    * Convert all information to a single buffer for writing to index
    * @return
    */
   public Buffer asByteBuffer() {
      byte[] axesKey = axesKey_.getBytes();
      byte[] filename = filename_.getBytes();

      int length =
              4 + axesKey.length +
              4 + filename.length +
              4 * 8;
      Buffer buffer = ByteBuffer.allocate(length).order(ByteOrder.nativeOrder());
      ((ByteBuffer) buffer).asIntBuffer().put(axesKey.length);
      buffer.position(buffer.position() + 4);
      ((ByteBuffer) buffer).put(axesKey);
      ((ByteBuffer) buffer).asIntBuffer().put(filename.length);
      buffer.position(buffer.position() + 4);
      ((ByteBuffer) buffer).put(filename);
      ((ByteBuffer) buffer).asIntBuffer().put(new int[]{(int) pixOffset_, (int) pixWidth_, (int) pixHeight_,
              (int) pixelType_, (int) pixelCompression_, (int) mdOffset_, (int) mdLength_, (int)
              mdCompression_});
      buffer.position(buffer.position() + 4 * 7);
      buffer.rewind();
      return buffer;
   }

   public static HashMap<String, Integer> deserializeAxes(String s) {
      try {
         JSONObject json = new JSONObject(s);
         HashMap<String, Integer> axes = new HashMap<String, Integer>();
         Iterator<String> keys = json.keys();
         keys.forEachRemaining(new Consumer<String>() {
            @Override
            public void accept(String s) {
               try {
                  axes.put(s, json.getInt(s));
               } catch (JSONException e) {
                  throw new RuntimeException(e);
               }
            }
         });
         return axes;
      } catch (JSONException e) {
         throw new RuntimeException(e);
      }
   }

   public static String serializeAxes(HashMap<String, Integer> axes) {
      try {
         JSONObject json = new JSONObject();
         //put into a new set to sort
         for (String key : new TreeSet<String>(axes.keySet())) {
            json.put(key, axes.get(key));
         }
         return json.toString();
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

}
