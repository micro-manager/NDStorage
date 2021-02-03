package org.micromanager.multiresstorage;

import javax.swing.*;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashSet;

public class IndexWriter {
   /**
    * This class writes a sperate index file with all information needed for fast access into the tiff file
    */

   private static final long BYTES_PER_MEG = 1048576;
   private static final long INITIAL_FILE_SIZE = 25*BYTES_PER_MEG;

   private RandomAccessFile raFile_;
   private FileChannel fileChannel_;
   private MappedByteBuffer mappedByteBuffer_;

   public IndexWriter(String directory) throws IOException {
      directory += (directory.endsWith(File.separator) ? "" : File.separator);
      String filename = "NDTiff.index";
      File f = new File(directory + "/" + filename);

      f.createNewFile();
      raFile_ = new RandomAccessFile(f, "rw");
      try {
         raFile_.setLength(INITIAL_FILE_SIZE);
         fileChannel_ = raFile_.getChannel();
         mappedByteBuffer_ = fileChannel_.map(FileChannel.MapMode.READ_WRITE, 0, INITIAL_FILE_SIZE);
      } catch (IOException e) {
         SwingUtilities.invokeLater(new Runnable() {
            @Override
            public void run() {
               JOptionPane.showMessageDialog(null, "Insufficient space on disk to write data",
                       "Error", JOptionPane.ERROR_MESSAGE);
            }
         });
         throw new RuntimeException("Insufficent space on disk: no room to write data");
      }
   }

   public void addEntry(IndexEntryData i) throws IOException {
//      fileChannel_.write((ByteBuffer) i.asByteBuffer());
      mappedByteBuffer_.put((ByteBuffer) i.asByteBuffer());
      //TODO: might need t check for overflow if mem mapping
   }

public void finishedWriting()  {
   try {
//      int writtenLength = (int) fileChannel_.position();
      int writtenLength = (int) mappedByteBuffer_.position();
      raFile_.setLength(writtenLength);
      fileChannel_.close();
//      raFile_.setLength(mappedByteBuffer_.position());
      raFile_.close();
   } catch (IOException ex) {
      ex.printStackTrace();
      throw new RuntimeException(ex);
   }
}

}
