package org.micromanager.multiresstorage;

import javax.swing.*;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;

public class AxesMetaDataWriter {

   private static final long BYTES_PER_MEG = 1048576;
   private static final long INITIAL_FILE_SIZE = 25*BYTES_PER_MEG;

   private RandomAccessFile raFile_;
   private FileChannel fileChannel_;
   private HashSet<String> channelNames_ = new HashSet<String>();


   public AxesMetaDataWriter(String directory) throws IOException {
      String filename = "Axes_metedata";
      File f = new File(directory + "/" + filename);

      f.createNewFile();
      raFile_ = new RandomAccessFile(f, "rw");
      try {
         raFile_.setLength(INITIAL_FILE_SIZE);
         fileChannel_ = raFile_.getChannel();
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

   public void addEntry(int channelIndex, int superChannelIndex,
                        String channelName, String superChannelName) throws IOException {
      if (!channelNames_.contains(channelName)) {
         // Add metadata about channel name and index
         Buffer flagAndIndexAndLength = ByteBuffer.allocate(12).order(ByteOrder.nativeOrder());
         ((ByteBuffer) flagAndIndexAndLength).asIntBuffer().put(new int[]{-1, channelIndex, channelName.length()});
         Buffer stringBuffer = ByteBuffer.wrap(channelName.getBytes(StandardCharsets.ISO_8859_1));
         fileChannel_.write(new ByteBuffer[]{(ByteBuffer)  flagAndIndexAndLength, (ByteBuffer) stringBuffer});
         channelNames_.add(channelName);
      }
      Buffer indexAndLength = ByteBuffer.allocate(8).order(ByteOrder.nativeOrder());
      ((ByteBuffer) indexAndLength).asIntBuffer().put(new int[]{superChannelIndex, superChannelName.length()});
      Buffer stringBuffer = ByteBuffer.wrap(superChannelName.getBytes(StandardCharsets.ISO_8859_1));
      fileChannel_.write(new ByteBuffer[]{(ByteBuffer)  indexAndLength, (ByteBuffer) stringBuffer});
   }

public void finishedWriting()  {
   try {
      raFile_.setLength(fileChannel_.position());
      raFile_.close();
   } catch (IOException ex) {
      throw new RuntimeException(ex);
   }
}

}
