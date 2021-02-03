package org.micromanager.multiresstorage;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Random;

public class SpeedTest {

   private static final int BYTES_PER_MEG = 1048576;
   private static final long BYTES_PER_GIG = 1073741824;


   public static void main (String[] args) throws Exception {
      File f = new File("/Users/henrypinkard/tmp/speedTest" + new Random().nextInt());
      long intitalLength = 20 * BYTES_PER_GIG;
      int chunkSize = BYTES_PER_MEG;
      int numChunks = 1024 * 5;


      RandomAccessFile r  = new RandomAccessFile(f, "rw");
      r.setLength(intitalLength);
      FileChannel fc = r.getChannel();

      ByteBuffer buff = ByteBuffer.allocateDirect(chunkSize);
      byte[] b = new byte[chunkSize];
      new Random().nextBytes(b);
      buff.put(b);

      ArrayList<Long> writeTimes = new ArrayList<Long>();

//      long d0 = System.nanoTime();
//      MappedByteBuffer mbb = fc.map(FileChannel.MapMode.READ_WRITE, 0, BYTES_PER_GIG);
//      System.out.println("Map time (ms): " + ((System.nanoTime() - d0) / 1000000.0));

      long e0 = System.nanoTime();
      for (int i = 0; i < numChunks; i++) {

//         buff = ByteBuffer.allocateDirect(1024 * 1024);
         byte[] bb = new byte[1024 * 1024];
         System.arraycopy(b,0,bb,0,b.length);
         buff.rewind();
         buff.put(bb);

         buff.rewind();
         fc.write(buff);
//         fc.write(buff, Math.abs(new Random().nextInt()));
//         mbb.put(buff);
//         mbb.position(i * 1024 *1024);
         long e1 = System.nanoTime();
         writeTimes.add(e1 - e0);
         e0 = e1;
      }
//      mbb.force();
      fc.force(true);
      r.close();

      double scale = 1000000.0;
      String unit = "ms";

      double closeTime = (System.nanoTime() - e0) / scale;
      double totalTime = closeTime + writeTimes.stream().mapToLong(l -> l).sum() / scale;
      double avgTime = writeTimes.stream().mapToLong(l -> l).sum() / writeTimes.size() / scale;
      double minTime = writeTimes.stream().mapToLong(l -> l).min().getAsLong() / scale;
      double maxTime = writeTimes.stream().mapToLong(l -> l).max().getAsLong() / scale;
      System.out.println(
              "Total (" + unit + "): \t" + totalTime + "\n" +
                      "Close (" + unit + "): \t" + closeTime + "\n" +
                      "Min (" + unit + "): \t" + minTime + "\n" +
                      "Max (" + unit + "): \t" + maxTime + "\n" +
                      "Avg (" + unit + "): \t" + avgTime );
      System.out.println("GB per s: " + numChunks / totalTime * 1000 / 1024 );
   }
}
