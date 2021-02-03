///////////////////////////////////////////////////////////////////////////////
//FILE:          MultipageTiffReader.java
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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import mmcorej.TaggedImage;
import mmcorej.org.json.JSONException;
import mmcorej.org.json.JSONObject;


public class MultipageTiffReader {
      
   private static final long BIGGEST_INT_BIT = (long) Math.pow(2, 31);

   
   public static final char BITS_PER_SAMPLE = MultipageTiffWriter.BITS_PER_SAMPLE;
   public static final char STRIP_OFFSETS = MultipageTiffWriter.STRIP_OFFSETS;    
   public static final char SAMPLES_PER_PIXEL = MultipageTiffWriter.SAMPLES_PER_PIXEL;
   public static final char STRIP_BYTE_COUNTS = MultipageTiffWriter.STRIP_BYTE_COUNTS;
   public static final char IMAGE_DESCRIPTION = MultipageTiffWriter.IMAGE_DESCRIPTION;
      public static final char WIDTH = 256;
   public static final char HEIGHT = 257;
   
   public static final char MM_METADATA = MultipageTiffWriter.MM_METADATA;
   
   private ByteOrder byteOrder_;  
   private File file_;
   private RandomAccessFile raFile_;
   private FileChannel fileChannel_;
      
   private JSONObject summaryMetadata_;
   private ConcurrentHashMap<String,IndexEntryData> indexMap_;

   /**
    * This constructor is used for a file that is currently being written
    */
   public MultipageTiffReader(JSONObject summaryMD) {
      summaryMetadata_ = summaryMD;
      byteOrder_ = MultiResMultipageTiffStorage.BYTE_ORDER;
   }

   /**
    * This constructor is used for opening datasets that have already been saved
    */
   public MultipageTiffReader(File file) throws IOException {
      file_ = file;
      try {
         createFileChannel(file_);
      } catch (Exception ex) {
         throw new RuntimeException("Can't successfully open file: " +  file_.getName());
      }
      long firstIFD = readHeader();
      indexMap_ = new ConcurrentHashMap<>();
      summaryMetadata_ = readSummaryMD();
//      if (summaryMetadata_ != null) {
//         getRGBAndByteDepth(summaryMetadata_);
//      }
   }

   public void setIndexMap(ConcurrentHashMap<String,IndexEntryData> indexMap) {
      indexMap_ = indexMap;
   }

   public ConcurrentHashMap<String,IndexEntryData> getIndexMap() {
      return indexMap_;
   }
   
   public void setFileChannel(FileChannel fc) {
      fileChannel_ = fc;
   }

   
   public static boolean isMMMultipageTiff(String directory) throws IOException {
      File dir = new File(directory);
      File[] children = dir.listFiles();
      File testFile = null;
      for (File child : children) {
         if (child.isDirectory()) {
            File[] grandchildren = child.listFiles();
            for (File grandchild : grandchildren) {
               if (grandchild.getName().endsWith(".tif")) {
                  testFile = grandchild;
                  break;
               }
            }
         } else if (child.getName().endsWith(".tif") || child.getName().endsWith(".TIF")) {
            testFile = child;
            break;
         }
      }
      if (testFile == null) {
         throw new IOException("Unexpected file structure: is this an MM dataset?");
      }
      RandomAccessFile ra;
      try {
         ra = new RandomAccessFile(testFile,"r");
      } catch (FileNotFoundException ex) {
        throw new RuntimeException(ex);
      }
      FileChannel channel = ra.getChannel();
      ByteBuffer tiffHeader = ByteBuffer.allocate(36);
      ByteOrder bo;
      channel.read(tiffHeader,0);
      char zeroOne = tiffHeader.getChar(0);
      if (zeroOne == 0x4949 ) {
         bo = ByteOrder.LITTLE_ENDIAN;
      } else if (zeroOne == 0x4d4d ) {
         bo = ByteOrder.BIG_ENDIAN;
      } else {
         throw new IOException("Error reading Tiff header");
      }
      tiffHeader.order(bo);
      int summaryMDHeader = tiffHeader.getInt(32);
      channel.close();
      ra.close();
      if (summaryMDHeader == MultipageTiffWriter.SUMMARY_MD_HEADER) {
         return true;
      }
      return false;
   }

//   private void getRGBAndByteDepth(JSONObject md) {
//      try {
//         String pixelType = md.getString("PixelType");
//         rgb_ = pixelType.startsWith("RGB");
//         
//            if (pixelType.equals("RGB32") || pixelType.equals("GRAY8")) {
//               byteDepth_ = 1;
//            } else {
//               byteDepth_ = 2;
//            }
//      } catch (Exception ex) {
//         throw new RuntimeException(ex);
//      }
//   }

   public JSONObject getSummaryMetadata() {
      return summaryMetadata_;
   }
   
   public TaggedImage readImage(String label) {
      if (indexMap_.containsKey(label)) {
         if (fileChannel_ == null) {
            throw new RuntimeException("Attempted to read image on FileChannel that is null"); //can happen on acquiition abort
         }
         try {
            return readTaggedImage(indexMap_.get(label));
         } catch (IOException ex) {
            throw new RuntimeException(ex);
         }
         
      } else {
         //label not in map--either writer hasnt finished writing it 
         return null;
      }
   }  
   
   public Set<String> getIndexKeys() {
      if (indexMap_ == null)
         return null;
      return indexMap_.keySet();
   }

   private JSONObject readSummaryMD() {
      try {
         ByteBuffer mdInfo = ByteBuffer.allocate(8).order(byteOrder_);
         fileChannel_.read(mdInfo, 16);
         int header = mdInfo.getInt(0);
         int length = mdInfo.getInt(4);
         
         if (header != MultipageTiffWriter.SUMMARY_MD_HEADER) {
            throw new RuntimeException("Summary Metadata Header Incorrect");
         }

         ByteBuffer mdBuffer = ByteBuffer.allocate(length).order(byteOrder_);
         fileChannel_.read(mdBuffer, 24);
         JSONObject summaryMD = new JSONObject(getString(mdBuffer));

         return summaryMD;
      } catch (Exception ex) {
         throw new RuntimeException("Couldn't read summary Metadata from file: " + file_.getName());
      }
   }
   
   private ByteBuffer readIntoBuffer(long position, int length) throws IOException {
      ByteBuffer buffer = ByteBuffer.allocate(length).order(byteOrder_);
      fileChannel_.read(buffer, position);
      return buffer;
   }
   
   private long readOffsetHeaderAndOffset(int offsetHeaderVal, int startOffset) throws IOException  {
      ByteBuffer buffer1 = readIntoBuffer(startOffset,8);
      int offsetHeader = buffer1.getInt(0);
      if ( offsetHeader != offsetHeaderVal) {
         throw new IOException("Offset header incorrect, expected: " + offsetHeaderVal +"   found: " + offsetHeader);
      }
      return unsignInt(buffer1.getInt(4));     
   }

   private IFDData readIFD(long byteOffset) throws IOException {
      ByteBuffer buff = readIntoBuffer(byteOffset,2);
      int numEntries = buff.getChar(0);
     
      ByteBuffer entries = readIntoBuffer(byteOffset + 2, numEntries*12 + 4).order(byteOrder_);
      IFDData data = new IFDData();
      for (int i = 0; i < numEntries; i++) {
         IFDEntry entry = readDirectoryEntry(i*12, entries);
         if (entry.tag == MM_METADATA) {
            data.mdOffset = entry.value;
            data.mdLength = entry.count;
         } else if (entry.tag == STRIP_OFFSETS) {
            data.pixelOffset = entry.value;
         } else if (entry.tag == STRIP_BYTE_COUNTS) {
            data.bytesPerImage = entry.value;
         } else if (entry.tag == WIDTH) {
            data.width = entry.value;
         } else if (entry.tag == HEIGHT) {
            data.height = entry.value;
         }
      }
      data.nextIFD = unsignInt(entries.getInt(numEntries*12));
      data.nextIFDOffsetLocation = byteOffset + 2 + numEntries*12;
      return data;
   }

   private String getString(ByteBuffer buffer) {
      try {
         return new String(buffer.array(), "UTF-8");
      } catch (UnsupportedEncodingException ex) {
         throw new RuntimeException(ex);
      }
   }
   
   private TaggedImage readTaggedImage(IndexEntryData data) throws IOException {
      int numBytes = (int) (data.pixWidth_ * data.pixHeight_ *
                    (data.pixelType_ == IndexEntryData.SIXTEEN_BIT ? 2 :
                            (data.pixelType_ == IndexEntryData.EIGHT_BIT ? 1 : 3)));
      ByteBuffer pixelBuffer = ByteBuffer.allocate(numBytes).order(byteOrder_);
      ByteBuffer mdBuffer = ByteBuffer.allocate((int) data.mdLength_).order(byteOrder_);
      fileChannel_.read(pixelBuffer, data.pixOffset_);
      fileChannel_.read(mdBuffer, data.mdOffset_);
      JSONObject md = new JSONObject();
      try {
         md = new JSONObject(getString(mdBuffer));
      } catch (JSONException ex) {
         throw new RuntimeException("Error reading image metadata from file");
      }
//      if ( byteDepth_ == 0) {
//         getRGBAndByteDepth(md);
//      }
      
      if (data.isRGB()) {
         if (data.getByteDepth() == 1) {
            byte[] pixels = new byte[numBytes];
            int i = 0;
            for (byte b : pixelBuffer.array()) {
               if (i % 4 == 0) {
                  i++;
               }
               pixels[i] = b;
               i++;
            }
            return new TaggedImage(pixels, md);
         }
         throw new RuntimeException("Only rgb32 supported");
//         else {
//            short[] pixels = new short[(int) (2 * (data.pixLength_/3))];
//            int i = 0;
//            while ( i < pixels.length) {
//               pixels[i] = pixelBuffer.getShort( 2*((i/4)*3 + (i%4)) );
//               i++;
//               if ((i + 1) % 4 == 0) {
//                  pixels[i] = 0;
//                  i++;
//               }
//            }
//            return new TaggedImage(pixels, md);
//         }
      } else {
         if (data.getByteDepth() == 1) {
            return new TaggedImage(pixelBuffer.array(), md);
         } else if (data.getByteDepth() == 2) {
            short[] pix = new short[pixelBuffer.capacity()/2];
            for (int i = 0; i < pix.length; i++ ) {
               pix[i] = pixelBuffer.getShort(i*2);
            }
            return new TaggedImage(pix, md);
         } else {
            System.err.println("invalid byte depth");
            throw new RuntimeException("invalid byte depth");
         }
      }
   }

   private IFDEntry readDirectoryEntry(int offset, ByteBuffer buffer) throws IOException {
      char tag =  buffer.getChar(offset); 
      char type = buffer.getChar(offset + 2);
      long count = unsignInt( buffer.getInt(offset + 4) );
      long value;
      if ( type == 3 && count == 1) {
         value = buffer.getChar(offset + 8);
      } else {
         value = unsignInt(buffer.getInt(offset + 8));
      }
      return (new IFDEntry(tag,type,count,value));
   }

   //returns byteoffset of first IFD
   private long readHeader() throws IOException {           
      ByteBuffer tiffHeader = ByteBuffer.allocate(8);
      fileChannel_.read(tiffHeader,0);
      char zeroOne = tiffHeader.getChar(0);
      if (zeroOne == 0x4949 ) {
         byteOrder_ = ByteOrder.LITTLE_ENDIAN;
      } else if (zeroOne == 0x4d4d ) {
         byteOrder_ = ByteOrder.BIG_ENDIAN;
      } else {
         throw new IOException("Error reading Tiff header");
      }
      tiffHeader.order( byteOrder_ );  
      short twoThree = tiffHeader.getShort(2);
      if (twoThree != 42) {
         throw new IOException("Tiff identifier code incorrect");
      }
      return unsignInt(tiffHeader.getInt(4));
   }
   
   private byte[] getBytesFromString(String s) {
      try {
         return s.getBytes("UTF-8");
      } catch (UnsupportedEncodingException ex) {
         throw new RuntimeException("Error encoding String to bytes");
      }
   }
   
   private void createFileChannel(File file) throws FileNotFoundException, IOException {      
      raFile_ = new RandomAccessFile(file,"r");
      fileChannel_ = raFile_.getChannel();
   }
   
   public void close() throws IOException {
      if (fileChannel_ != null) {
         fileChannel_.close();
         fileChannel_ = null;
      }
      if (raFile_ != null) {
         raFile_.close();
         raFile_ = null;
      }
   }
      
   private long unsignInt(int i) {
      long val = Integer.MAX_VALUE & i;
      if (i < 0) {
         val += BIGGEST_INT_BIT;
      }
      return val;
   }

   private class IFDData {
      public long pixelOffset;
      public long bytesPerImage;
      public long mdOffset;
      public long mdLength;
      public long nextIFD;
      public long nextIFDOffsetLocation;
      public long width, height;
      
      public IFDData() {}
   }
   
   private class IFDEntry {
      public char tag, type;
      public long count, value;
      
      public IFDEntry(char tg, char typ, long cnt, long val) {
         tag = tg;
         type = typ;
         count = cnt;
         value = val;
      }
   }
 
   
}
