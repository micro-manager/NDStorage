package org.micromanager.multiresstorage;

import javax.swing.*;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class AxesMetaDataWriter {

   private static final long BYTES_PER_MEG = 1048576;
   private static final long INITIAL_FILE_SIZE = 25*BYTES_PER_MEG;

   private RandomAccessFile raFile_;
   private FileChannel fileChannel_;
   private ExecutorService writingExecutor_;
   private long filePosition_ = 0;
   private long indexMapPosition_; //current position of the dynamically written index map
   private long indexMapFirstEntry_; // mark position of first entry so that number of entries can be written at end
   private int bufferPosition_;


   public AxesMetaDataWriter(String directory, ExecutorService writingExecutor) throws IOException {
      writingExecutor_ = writingExecutor;
      String filename = "Axes_metedata.txt";
      File f = new File(directory + "/" + filename);

      f.createNewFile();
      raFile_ = new RandomAccessFile(f, "rw");
      try {
         raFile_.setLength(INITIAL_FILE_SIZE);
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
   }

   public void addEntry(HashMap<String, Integer> axes) {
      //TODO: write the data
      long offset = filePosition_;
//      boolean shiftByByte = writeIFD(img);
//
//      Future f = writeBuffers();
//
//      String label = c + "_" + z + "_" + t + "_" + p;
//
//      addToIndexMap(label, offset);
//      //Make IFDs start on word
//      if (shiftByByte) {
//
//         f = executeWritingTask(new Runnable() {
//            @Override
//            public void run() {
//               try {
//                  fileChannel_.position(fileChannel_.position() + 1);
//               } catch (IOException ex) {
//                  throw new RuntimeException("Couldn't incremement byte");
//               }
//            }
//         });
//
//      return f;
   }

//TODO keep track of file length

public void finishedWriting() throws IOException {
   try {
      //extra byte of space, just to make sure nothing gets cut off
      raFile_.setLength(filePosition_ + 8);
   } catch (IOException ex) {
      throw new RuntimeException(ex);
   }
   raFile_.close();
}

}
