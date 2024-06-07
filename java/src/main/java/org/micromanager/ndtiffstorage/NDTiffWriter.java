///////////////////////////////////////////////////////////////////////////////
//FILE:          MultipageTiffWriter.java
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

import java.io.*;
import java.nio.*;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.*;

import mmcorej.org.json.JSONException;
import mmcorej.org.json.JSONObject;

import javax.swing.*;

public class NDTiffWriter {

//   private static final long BYTES_PER_MEG = 1048576;
//   private static final long MAX_FILE_SIZE = 15*BYTES_PER_MEG;
   private static final long BYTES_PER_GIG = 1073741824;
   private static final long MAX_FILE_SIZE = 4 * BYTES_PER_GIG;

   public static final char ENTRIES_PER_IFD = 13;
   //Required tags
   public static final char WIDTH = 256;
   public static final char HEIGHT = 257;
   public static final char BITS_PER_SAMPLE = 258;
   public static final char COMPRESSION = 259;
   public static final char PHOTOMETRIC_INTERPRETATION = 262;
   public static final char IMAGE_DESCRIPTION = 270;
   public static final char STRIP_OFFSETS = 273;
   public static final char SAMPLES_PER_PIXEL = 277;
   public static final char ROWS_PER_STRIP = 278;
   public static final char STRIP_BYTE_COUNTS = 279;
   public static final char X_RESOLUTION = 282;
   public static final char Y_RESOLUTION = 283;
   public static final char RESOLUTION_UNIT = 296;
   public static final char MM_METADATA = 51123;

   public static final int SUMMARY_MD_HEADER = 2355492;


   private NDTiffStorage masterMPTiffStorage_;
   private RandomAccessFile raFile_;
   private FileChannel fileChannel_;
   private ConcurrentHashMap<String, IndexEntryData> indexMap_;
   private long nextIFDOffsetLocation_ = -1;
   private long resNumerator_ = 1, resDenomenator_ = 1;
   private double zStepUm_ = 1;
   private LinkedList<ByteBuffer> buffers_;
   private boolean firstIFD_ = true;
   //Reader associated with this file
   private NDTiffReader reader_;
   private final String filename_;

   private long startTime_;


   public NDTiffWriter(String directory, String filename,
                       JSONObject summaryMD, NDTiffStorage mpTiffStorage) throws IOException {
      masterMPTiffStorage_ = mpTiffStorage;
      reader_ = new NDTiffReader(summaryMD);
      File f = new File(directory + "/" + filename);
      filename_ = directory + "/" + filename;


      //this is just for optional tiff stuff now
       processSummaryMD(summaryMD);
      
      //This is an overestimate of file size because file gets truncated at end
//      long fileSize = Math.min(MAX_FILE_SIZE, summaryMD.toString().length() + 2000000
//              + numFrames_ * numChannels_ * numSlices_ * ((long) bytesPerImagePixels_ + 2000));
      //just set it to the max, since we don't really know in advance how many frames there are, and
      //we dont want to slow down performance by continually making calls to the OS to expand the fie
      long fileSize = MAX_FILE_SIZE;

      f.createNewFile();
      raFile_ = new RandomAccessFile(f, "rw");
      try {
         raFile_.setLength(fileSize);
      } catch (IOException e) {
         new Thread(new Runnable() {

            @Override
            public void run() {
               try {
                  Thread.sleep(1000);
               } catch (InterruptedException ex) {
               }
            }
         }).start();
         JOptionPane.showMessageDialog(null, "Insufficient space on disk to write data",
                 "Error", JOptionPane.ERROR_MESSAGE);
         throw new RuntimeException("Insufficent space on disk: no room to write data");
      }
      fileChannel_ = raFile_.getChannel();
      indexMap_ = new ConcurrentHashMap<String, IndexEntryData>();
      reader_.setFileChannel(fileChannel_);
      reader_.setIndexMap(indexMap_);
      buffers_ = new LinkedList<ByteBuffer>();

      startTime_ = System.nanoTime();
      writeMMHeaderAndSummaryMD(summaryMD);
   }

   private void fileChannelWrite(final Buffer buffer, final long position) {
      try {
         buffer.rewind();
         fileChannel_.write((ByteBuffer) buffer, position);
      } catch (ClosedChannelException e) {
         throw new RuntimeException(e);
      } catch (IOException e) {
         throw new RuntimeException(e);
      }
      masterMPTiffStorage_.tryRecycleLargeBuffer((ByteBuffer) buffer);
   }


   private void fileChannelWriteSequential(final ByteBuffer[] buffers) {

      try {
         long numBytesWritten = fileChannel_.write(buffers);
      } catch (IOException e) {
         throw new RuntimeException(e);
      }
      for (ByteBuffer buffer : buffers) {
         masterMPTiffStorage_.tryRecycleLargeBuffer(buffer);
      }
   }

   public NDTiffReader getReader() {
      return reader_;
   }

   public ConcurrentHashMap<String, IndexEntryData> getIndexMap() {
      return indexMap_;
   }

   private void writeMMHeaderAndSummaryMD(JSONObject summaryMD) throws IOException {
      byte[] summaryMDBytes = getBytesFromString(summaryMD.toString());
      int mdLength = summaryMDBytes.length;
      //28 bytes
      ByteBuffer headerBuffer = (ByteBuffer) masterMPTiffStorage_.getSmallBuffer(28);
      //8 bytes for file header
      if (masterMPTiffStorage_.BYTE_ORDER.equals(ByteOrder.BIG_ENDIAN)) {
         headerBuffer.asCharBuffer().put(0, (char) 0x4d4d);
      } else {
         headerBuffer.asCharBuffer().put(0, (char) 0x4949);
      }
      headerBuffer.asCharBuffer().put(1, (char) 42);
      int firstIFDOffset = 28 + (int) (mdLength);
      if (firstIFDOffset % 2 == 1) {
         firstIFDOffset++; //Start first IFD on a word
      }
      headerBuffer.putInt(4, firstIFDOffset);


      //12 bytes for unique identifier and major version
      headerBuffer.putInt(8, 483729);
      headerBuffer.putInt(12, VERSION.MAJOR);
      headerBuffer.putInt(16, VERSION.MINOR);

      //8 bytes for summaryMD header  summary md length + 
      headerBuffer.putInt(20, SUMMARY_MD_HEADER);
      headerBuffer.putInt(24, mdLength);


      //1 byte for each byte of UTF-8-encoded summary md
      ByteBuffer[] buffers = new ByteBuffer[2];
      buffers[0] = headerBuffer;
      buffers[1] = ByteBuffer.wrap(summaryMDBytes);

      fileChannelWriteSequential(buffers);
   }

   /**
    * Called when there is no more data to be written. Write null offset after
    * last image in accordance with TIFF specification and set number of index
    * map entries for backwards reading capability A file that has been finished
    * should have everything it needs to be properly reopened in MM or by a
    * basic TIFF reader
    */
   public void finishedWriting() throws IOException, ExecutionException, InterruptedException {
      writeNullOffsetAfterLastImage();
      try {
         raFile_.setLength(fileChannel_.position());
      } catch (IOException ex) {
         throw new RuntimeException(ex);
      }

      double gbPerS = ( (double)raFile_.length() / (double) ( System.nanoTime() - startTime_)) / 1024 / 1024 / 1024 * 1000 * 1000 *1000;
      if (masterMPTiffStorage_.debugLogger_ != null) {
         masterMPTiffStorage_.debugLogger_.accept("Speed (GB/s)  " + gbPerS);
      }
      raFile_ = null;
      fileChannel_ = null;
   }

   /**
    * Called when entire set of files (i.e. acquisition) is finished
    */
   public void close() throws IOException, InterruptedException, ExecutionException {
//            reader_.finishedWriting();
      //Dont close file channel and random access file becase Tiff reader still using them
      fileChannel_ = null;
      raFile_ = null;
   }

   private int bytesPerImagePixels(Object pixels, boolean rgb) {
      int bytesPerPixels;
      if (rgb) {
         bytesPerPixels = ((byte[]) pixels).length / 4 * 3;
      } else {
         if (pixels instanceof byte[]) {
            bytesPerPixels = ((byte[]) pixels).length;
         } else if (pixels instanceof short[]) {
            bytesPerPixels = ((short[]) pixels).length * 2;
         } else if (pixels instanceof int[]) {
            bytesPerPixels = ((int[]) pixels).length * 4;
         } else if (pixels instanceof float[]) {
            bytesPerPixels = ((float[]) pixels).length * 4;
         } else {
            throw new RuntimeException("unknown pixel type");
         }
      }
      return  bytesPerPixels;
   }

   public boolean hasSpaceToWrite(Object pixels, byte[] metadata, boolean rgb) throws IOException {
      int mdLength = metadata.length;
      int IFDSize = ENTRIES_PER_IFD * 12 + 4 + 16;
      //5 MB extra padding...just to be safe...
      int extraPadding = 5000000;
      long bytesPerPixels = bytesPerImagePixels(pixels, rgb);
      long size = mdLength + IFDSize + bytesPerPixels + extraPadding + fileChannel_.position();

      if (size >= MAX_FILE_SIZE) {
         return false;
      }
      return true;
   }

   public boolean isClosed() {
      return raFile_ == null;
   }

   public IndexEntryData writeImage(String indexKey, Object pixels, byte[] metadata,
                                    boolean rgb, int imageHeight, int imageWidth, int bitDepth) throws IOException {
      IndexEntryData ied = writeIFD(indexKey, pixels, metadata, rgb, imageHeight, imageWidth, bitDepth);
      writeBuffers();
      indexMap_.put(indexKey, ied);

      return ied;
   }

   private void writeBuffers() {
      ByteBuffer[] buffs = new ByteBuffer[buffers_.size()];
      for (int i = 0; i < buffs.length; i++) {
         buffs[i] = buffers_.removeFirst();
      }
      fileChannelWriteSequential(buffs);
   }

   private long unsignInt(int i) {
      long val = Integer.MAX_VALUE & i;
      if (i < 0) {
         val += (long) Math.pow(2, 31);
      }
      return val;
   }

   public void overwritePixels(String indexKey, Object pixels, boolean rgb) throws IOException {
      long pixelOffset = indexMap_.get(indexKey).pixOffset_;
      Buffer pixBuff = getPixelBuffer(pixels, rgb);
      fileChannelWrite(pixBuff, pixelOffset);
   }

   private IndexEntryData writeIFD(String indexKey, Object pixels, byte[] metadata,
                                   boolean rgb, int imageHeight, int imageWidth, int bitDepth
                            ) throws IOException {

      if (fileChannel_.position() % 2 == 1) {
         fileChannel_.position(fileChannel_.position() + 1); //Make IFD start on word
      }

      int byteDepth = pixels instanceof byte[] ? 1 : (pixels instanceof short[] ? 2 : 1);
      int bytesPerImagePixels = bytesPerImagePixels(pixels, rgb);
      char numEntries = ENTRIES_PER_IFD;

      //2 bytes for number of directory entries, 12 bytes per directory entry, 4 byte offset of next IFD
      //6 bytes for bits per sample if RGB, 16 bytes for x and y resolution, 1 byte per character of MD string
      //number of bytes for pixels
//      int totalBytes = 2 + numEntries * 12 + 4 + (rgb ? 6 : 0) + 16 + metadata.length + bytesPerImagePixels;
      int IFDandBitDepthBytes = 2 + numEntries * 12 + 4 + (rgb ? 6 : 0) + 16;

      ByteBuffer ifdAndSmallValsBuffer = (ByteBuffer) masterMPTiffStorage_.getSmallBuffer(IFDandBitDepthBytes);
      CharBuffer charView = ifdAndSmallValsBuffer.asCharBuffer();

      //Needed to reset to zero after last IFD
      nextIFDOffsetLocation_ = fileChannel_.position() + 2 + numEntries * 12;
      //Locations of data outside the IFDs
      long bitsPerSampleOffset = nextIFDOffsetLocation_ + 4;
      long xResolutionOffset = bitsPerSampleOffset + (rgb ? 6 : 0);
      long yResolutionOffset = xResolutionOffset + 8;
      long pixelDataOffset = yResolutionOffset + 8;
      long metadataOffset = pixelDataOffset + bytesPerImagePixels(pixels, rgb);

      long nextIFDOffset = metadataOffset + metadata.length;
      if (nextIFDOffset % 2 == 1) {
         nextIFDOffset++; //Make IFD start on word (the filechannel itself handled elsewhere)
      }

      int bufferPosition = 0;
      charView.put(bufferPosition, numEntries);
      bufferPosition += 2;

      bufferPosition += writeIFDEntry(ifdAndSmallValsBuffer, charView, WIDTH, (char) 4, 1, imageWidth, bufferPosition);
      bufferPosition += writeIFDEntry(ifdAndSmallValsBuffer, charView, HEIGHT, (char) 4, 1, imageHeight, bufferPosition);
      bufferPosition += writeIFDEntry(ifdAndSmallValsBuffer, charView, BITS_PER_SAMPLE, (char) 3, rgb ? 3 : 1, rgb ? bitsPerSampleOffset : byteDepth * 8, bufferPosition);
      bufferPosition += writeIFDEntry(ifdAndSmallValsBuffer, charView, COMPRESSION, (char) 3, 1, 1, bufferPosition);
      bufferPosition += writeIFDEntry(ifdAndSmallValsBuffer, charView, PHOTOMETRIC_INTERPRETATION, (char) 3, 1, rgb ? 2 : 1, bufferPosition);
      bufferPosition += writeIFDEntry(ifdAndSmallValsBuffer, charView, STRIP_OFFSETS, (char) 4, 1, pixelDataOffset, bufferPosition);
      bufferPosition += writeIFDEntry(ifdAndSmallValsBuffer, charView, SAMPLES_PER_PIXEL, (char) 3, 1, (rgb ? 3 : 1), bufferPosition);
      bufferPosition += writeIFDEntry(ifdAndSmallValsBuffer, charView, ROWS_PER_STRIP, (char) 3, 1, imageHeight, bufferPosition);
      bufferPosition += writeIFDEntry(ifdAndSmallValsBuffer, charView, STRIP_BYTE_COUNTS, (char) 4, 1, bytesPerImagePixels, bufferPosition);
      bufferPosition += writeIFDEntry(ifdAndSmallValsBuffer, charView, X_RESOLUTION, (char) 5, 1, xResolutionOffset, bufferPosition);
      bufferPosition += writeIFDEntry(ifdAndSmallValsBuffer, charView, Y_RESOLUTION, (char) 5, 1, yResolutionOffset, bufferPosition);
      bufferPosition += writeIFDEntry(ifdAndSmallValsBuffer, charView, RESOLUTION_UNIT, (char) 3, 1, 3, bufferPosition);
      bufferPosition += writeIFDEntry(ifdAndSmallValsBuffer, charView, MM_METADATA, (char) 2, metadata.length, metadataOffset, bufferPosition);

      ifdAndSmallValsBuffer.putInt(bufferPosition, (int) nextIFDOffset);
      bufferPosition += 4;

      if (rgb) {
         charView.put(bufferPosition / 2, (char) (byteDepth * 8));
         charView.put(bufferPosition / 2 + 1, (char) (byteDepth * 8));
         charView.put(bufferPosition / 2 + 2, (char) (byteDepth * 8));
         bufferPosition += 6;
      }

      ifdAndSmallValsBuffer.putInt(bufferPosition, (int) (int) resNumerator_);
      bufferPosition += 4;
      ifdAndSmallValsBuffer.putInt(bufferPosition, (int) (int) resDenomenator_);
      bufferPosition += 4;
      ifdAndSmallValsBuffer.putInt(bufferPosition, (int) (int) resNumerator_);
      bufferPosition += 4;
      ifdAndSmallValsBuffer.putInt(bufferPosition, (int) (int) resDenomenator_);
      bufferPosition += 4;

      buffers_.add(ifdAndSmallValsBuffer);


      Buffer pixBuff = getPixelBuffer(pixels, rgb);
      buffers_.add((ByteBuffer) pixBuff);
      buffers_.add(ByteBuffer.wrap(metadata));


      firstIFD_ = false;

      ///////   Return structured data for putting into the index entry //////////////
      int pixelType;
      if (rgb) {
         pixelType = IndexEntryData.EIGHT_BIT_RGB;
      } else if (bitDepth == 8) {
         pixelType = IndexEntryData.EIGHT_BIT;
      } else if (bitDepth == 10) {
         pixelType = IndexEntryData.TEN_BIT;
      } else if (bitDepth == 12) {
         pixelType = IndexEntryData.TWELVE_BIT;
      } else if (bitDepth == 14) {
         pixelType = IndexEntryData.FOURTEEN_BIT;
      } else if (bitDepth == 16) {
         pixelType = IndexEntryData.SIXTEEN_BIT;
      } else if (bitDepth == 11) {
         pixelType = IndexEntryData.ELEVEN_BIT;
      } else {
         pixelType = pixels instanceof byte[] ? IndexEntryData.EIGHT_BIT : IndexEntryData.SIXTEEN_BIT;
      }

      return new IndexEntryData(indexKey, pixelType, pixelDataOffset, imageWidth, imageHeight,
              metadataOffset, metadata.length, new File(filename_).getName());
   }

   private int writeIFDEntry(ByteBuffer buffer, CharBuffer cBuffer, char tag, char type, long count, long value,
                             int bufferPosition) throws IOException {
      cBuffer.put(bufferPosition / 2, tag);
      cBuffer.put(bufferPosition / 2 + 1, type);
      buffer.putInt(bufferPosition + 4, (int) count);
      if (type == 3 && count == 1) {  //Left justify in 4 byte value field
         cBuffer.put(bufferPosition / 2 + 4, (char) value);
         cBuffer.put(bufferPosition / 2 + 5, (char) 0);
      } else {
         buffer.putInt(bufferPosition + 8, (int) value);
      }
      return 12;
   }

   private ByteBuffer getPixelBuffer(Object pixels, boolean  rgb) throws IOException {
      try {
         if (rgb) {

            byte[] originalPix = (byte[]) pixels;
            byte[] rgbPix = new byte[originalPix.length * 3 / 4];
            int numPix = originalPix.length / 4;
            for (int tripletIndex = 0; tripletIndex < numPix; tripletIndex++) {
               //reorer colors so they save correctly
               rgbPix[tripletIndex * 3] = originalPix[tripletIndex * 4 + 2];
               rgbPix[tripletIndex * 3 + 1] = originalPix[tripletIndex * 4 + 1];
               rgbPix[tripletIndex * 3 + 2] = originalPix[tripletIndex * 4 ];
            }
            return ByteBuffer.wrap(rgbPix);
//         } 
//         else {
//            short[] originalPix = (short[]) pixels;
//            short[] rgbaPix = new short[originalPix.length * 3 / 4];
//            int count = 0;
//            for (int i = 0; i < originalPix.length; i++) {
//               if ((i + 1) % 4 != 0) {
//                  //swap R and B for correct format
//                  if ((i + 1) % 4 == 1 ) {
//                     rgbaPix[count] = originalPix[i + 2];
//                  } else if ((i + 1) % 4 == 3) {
//                     rgbaPix[count] = originalPix[i - 2];
//                  } else {                      
//                     rgbaPix[count] = originalPix[i];
//                  }
//                  count++;
//               }
//            }
//            ByteBuffer buffer = allocateByteBufferMemo(rgbaPix.length * 2);
//            buffer.rewind();
//            buffer.asShortBuffer().put(rgbaPix);
//            return buffer;
//         }
         } else if (pixels instanceof byte[]) {
            return ByteBuffer.wrap((byte[]) pixels);
         } else {
//            System.out.println("Java version " + getVersion());
            short[] pix = (short[]) pixels;
            Buffer buffer = masterMPTiffStorage_.getLargeBuffer(pix.length * 2);
//            buffer.rewind();
            ((ByteBuffer) buffer).asShortBuffer().put(pix);
            return (ByteBuffer) buffer;
         }
      } catch (Exception e) {
         System.err.println(e);
         e.printStackTrace();
         throw new RuntimeException(e);
      }
   }

//   private static int getVersion() {
//      String version = System.getProperty("java.version");
//      if(version.startsWith("1.")) {
//         version = version.substring(2, 3);
//      } else {
//         int dot = version.indexOf(".");
//         if(dot != -1) { version = version.substring(0, dot); }
//      } return Integer.parseInt(version);
//   }

   private void processSummaryMD(JSONObject summaryMD) {
         //Tiff resolution tag values
         double cmPerPixel = 0.0001;
         if (summaryMD.has("PixelSizeUm")) {
            try {
               cmPerPixel = 0.0001 * summaryMD.getDouble("PixelSizeUm");
            } catch (JSONException ex) {
            }
         } else if (summaryMD.has("PixelSize_um")) {
            try {
               cmPerPixel = 0.0001 * summaryMD.getDouble("PixelSize_um");
            } catch (JSONException ex) {
            }
         }
         double log = Math.log10(cmPerPixel);
         if (log >= 0) {
            resDenomenator_ = (long) cmPerPixel;
            resNumerator_ = 1;
         } else {
            resNumerator_ = (long) (1 / cmPerPixel);
            resDenomenator_ = 1;
         }
//      zStepUm_ = AcqEngMetadata.getZStepUm(summaryMD);
   
   }

   private byte[] getBytesFromString(String s) {
      try {
         return s.getBytes("UTF-8");
      } catch (UnsupportedEncodingException ex) {
         throw new RuntimeException("Error encoding String to bytes");
      }
   }

   private void writeNullOffsetAfterLastImage() throws IOException, InterruptedException, ExecutionException {
      ByteBuffer buffer = (ByteBuffer) masterMPTiffStorage_.getSmallBuffer(4);
      buffer.putInt(0, 0);
      fileChannelWrite(buffer, nextIFDOffsetLocation_);
   }

}
