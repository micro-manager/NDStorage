package org.micromanager.multiresstorage;

import mmcorej.org.json.JSONObject;

import javax.swing.*;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashSet;

public class DisplaySettingsWriter {
   /**
    * This class writes a sperate index file with all information needed for fast access into the tiff file
    */


   private RandomAccessFile raFile_;
   private FileChannel fileChannel_;

   public DisplaySettingsWriter(String directory) {
      directory += (directory.endsWith(File.separator) ? "" : File.separator);
      String filename = "display_settings.txt";
      File f = new File(directory + "/" + filename);

      try {
         f.createNewFile();
         raFile_ = new RandomAccessFile(f, "rw");
         fileChannel_ = raFile_.getChannel();
      } catch (IOException e) {
         throw new RuntimeException(e);
      }

   }

   public void add(JSONObject displaySettings) throws IOException {
      fileChannel_.write(ByteBuffer.wrap(displaySettings.toString().getBytes()));
   }

   public void finishedWriting()  {
      try {
         fileChannel_.close();
         raFile_.close();
      } catch (IOException ex) {
         ex.printStackTrace();
         throw new RuntimeException(ex);
      }
   }

}
