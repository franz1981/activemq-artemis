/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.cli.commands.util;

import java.io.File;
import java.io.PrintStream;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.HdrHistogram.Histogram;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.core.io.SequentialFileFactory;
import org.apache.activemq.artemis.core.journal.impl.SyncIOCompletion;
import org.apache.activemq.artemis.core.server.JournalType;
import org.apache.activemq.artemis.utils.ReusableLatch;

/**
 * It will perform a simple test to evaluate how many syncs a disk can make per second
 * * *
 */
public final class JournalLatency {

   private JournalLatency() {

   }

   /**
    * It will perform {@code tries} write tests of {@code blockSize * blocks} bytes, printing the {@link Histogram} of the write latencies.
    *
    * <p>
    * Before each test it could (ie {@code journalType == JournalType.ASYNCIO && !syncWrites} perform (and await) several GCs to stabilize the results if any garbage is produced during the test.<br>
    * Please configure {@code blocks >= -XX:CompileThreshold} (ie by default on most JVMs is 10000) to favour the best JIT/OSR compilation (ie: Just In Time/On Stack Replacement)
    * if the test is running on a temporary file-system (eg: tmpfs on Linux) or without {@code fsync}.
    * <p>
    * NOTE: the write latencies printed are calculated using the elapsed time to call {@link IOCallback#done()} on each write.<br>
    * That means that with {@link JournalType#ASYNCIO} they are measured using the elapsed time to perform a write iff if it is being performed in sequence: the value is anyway
    * useful because it is the response time of the write as perceived by the caller.
    *
    * @param datafolder  the folder where the journal files will be stored
    * @param blockSize   the size in bytes of each write on the journal
    * @param blocks      the number of {@code blockSize} writes performed on each try
    * @param tries       the number of tests
    * @param out         where the histograms will be printed
    * @param err         where the errors will be printed
    * @param fsync       if {@code true} the test is performing full durable writes, {@code false} otherwise
    * @param syncWrites  if {@code true} each write is performed only if the previous one is completed, {@code false} otherwise (ie each try will wait only the last write)
    * @param fileName
    * @param maxAIO      the max number of in-flight IO requests (if {@code journalType} will support it)
    * @param journalType the {@link JournalType} used for the tests
    * @throws Exception
    */
   public static void measureWriteLatencyDistribution(File datafolder,
                                                      int blockSize,
                                                      int blocks,
                                                      int tries,
                                                      PrintStream out,
                                                      PrintStream err,
                                                      boolean fsync,
                                                      boolean syncWrites,
                                                      String fileName,
                                                      int maxAIO,
                                                      JournalType journalType) throws Exception {
      //it is safe to use (blockSize * blocks) here as the file size just because it will be used only by the MAPPED journal that has alignment === 1
      final SequentialFileFactory factory = SyncCalculation.newFactory(datafolder, fsync, journalType, blockSize * blocks, maxAIO);
      //initialize write buffer
      final ByteBuffer bufferBlock = factory.newBuffer(blockSize);
      //write enough data to fill the aligned buffer block
      final byte[] block = new byte[bufferBlock.limit()];
      Arrays.fill(block, (byte) 't');
      bufferBlock.put(block);
      bufferBlock.position(0);
      //the real block size depends on the alignment
      final int alignedBlockSize = bufferBlock.remaining();
      assert (alignedBlockSize % factory.getAlignment() == 0) : "the new buffer must be size-aligned";
      System.out.println("Using " + factory.getClass().getName() + " to calculate sync times, alignment=" + factory.getAlignment() + " block size=" + blockSize + " aligned block size=" + alignedBlockSize);
      final Histogram writeLatencies = new Histogram(MAX_FLUSH_NANOS, 2);
      final boolean asyncWrites = journalType == JournalType.ASYNCIO && !syncWrites;
      final Supplier<? extends SyncIOCompletion> ioCallbackFactory;
      if (asyncWrites) {
         //it is necessary to allocate new callbacks because each async write has its own elapsed time
         ioCallbackFactory = () -> new LatencyRecorderIOCallback(writeLatencies);
      } else {
         //i can reuse the same callback each time
         final ReusableIOCallback ioCallback = new ReusableIOCallback(writeLatencies);
         //uses will perform a countUp and reset start
         ioCallbackFactory = () -> {
            ioCallback.beforeWrite();
            return ioCallback;
         };
      }
      final SequentialFile file = factory.createSequentialFile(fileName);
      //to prevent the process/thread's crash to produce garbage files on dataFolder
      file.getJavaFile().deleteOnExit();

      try {
         file.delete();
         file.open();
         final int alignedFileSize = alignedBlockSize * blocks;
         file.fill(alignedFileSize);
         file.close();

         final DecimalFormat dcformat = new DecimalFormat("###.##");
         for (int ntry = 0; ntry < tries; ntry++) {
            //perform and wait GC on each test iteration to help cleanup of IO callbacks/garbage
            if (asyncWrites) {
               awaitGC(err);
            }
            out.println("**************************************************");
            out.println((ntry + 1) + " of " + tries + " calculation");
            file.open();
            file.position(0);
            final long start = System.currentTimeMillis();
            SyncIOCompletion lastIOCallback = null;
            for (int i = 0; i < blocks; i++) {
               //used a separate method to help compiler to peform JIT instead of ORS
               lastIOCallback = writeBlock(file, syncWrites, bufferBlock, ioCallbackFactory);
               assert ((journalType != JournalType.ASYNCIO && !bufferBlock.hasRemaining()) || (journalType == JournalType.ASYNCIO && bufferBlock.remaining() == alignedBlockSize)) : "ASYNCIO journal cannot modify bufferBlock position or limit";
            }
            //just wait the last callback to be completed in order to be sure the previous ones were fine
            if (!syncWrites && lastIOCallback != null) {
               lastIOCallback.waitCompletion();
            }/**/
            final long elapsedMillis = System.currentTimeMillis() - start;
            final double writesPerMillisecond = (double) blocks / (double) elapsedMillis;
            out.println("Time = " + elapsedMillis + " milliseconds");
            out.println("Writes / millisecond = " + dcformat.format(writesPerMillisecond));
            out.println("Write Latencies Percentile Distribution in microseconds");
            //print latencies in us -> (ns * 1000d)
            writeLatencies.outputPercentileDistribution(out, 1000d);
            writeLatencies.reset();
            out.println("**************************************************");

            file.close();
         }
         factory.releaseDirectBuffer(bufferBlock);
      } finally {
         try {
            file.close();
         } catch (Exception e) {
         }
         try {
            file.delete();
         } catch (Exception e) {
         }
         try {
            factory.stop();
         } catch (Exception e) {
         }
      }
   }

   private static final long MAX_FLUSH_NANOS = TimeUnit.SECONDS.toNanos(5);

   private static final class ReusableIOCallback extends SyncIOCompletion {

      private String errorMessage;
      private final ReusableLatch operationPerformed = new ReusableLatch(0);
      private long start;
      private final Histogram histogram;

      private ReusableIOCallback(Histogram histogram) {
         this.errorMessage = null;
         this.histogram = histogram;
      }

      public void beforeWrite() {
         operationPerformed.countUp();
         start = System.nanoTime();
      }

      @Override
      public void storeLineUp() {
         //no op
      }

      @Override
      public void done() {
         histogram.recordValue(System.nanoTime() - start);
         operationPerformed.countDown();
      }

      @Override
      public void onError(int errorCode, String errorMessage) {
         //no value recorded!
         this.errorMessage = errorMessage;
         operationPerformed.countDown();
      }

      @Override
      public void waitCompletion() throws Exception {
         if (this.operationPerformed.await(MAX_FLUSH_NANOS, TimeUnit.NANOSECONDS)) {
            if (errorMessage != null) {
               throw new IllegalStateException(errorMessage);
            }
         } else {
            throw new IllegalStateException("the IO operation hasn't finished");
         }
      }
   }

   private static final class LatencyRecorderIOCallback extends SyncIOCompletion {

      private final long start;
      private final CountDownLatch operationPerformed;
      private final Histogram histogram;
      private String errorMessage;

      private LatencyRecorderIOCallback(Histogram histogram) {
         this.operationPerformed = new CountDownLatch(1);
         this.histogram = histogram;
         this.start = System.nanoTime();
         this.errorMessage = null;
      }

      @Override
      public void storeLineUp() {
         //no op
      }

      @Override
      public void done() {
         histogram.recordValue(System.nanoTime() - start);
         //performing the countdown has its cost, but will impact only the IO throughput
         operationPerformed.countDown();
      }

      @Override
      public void onError(int errorCode, String errorMessage) {
         //no value recorded!
         this.errorMessage = errorMessage;
         operationPerformed.countDown();
      }

      @Override
      public void waitCompletion() throws Exception {
         if (this.operationPerformed.await(MAX_FLUSH_NANOS, TimeUnit.NANOSECONDS)) {
            if (errorMessage != null) {
               throw new IllegalStateException(errorMessage);
            }
         } else {
            throw new IllegalStateException("the IO operation hasn't finished");
         }
      }
   }

   private static SyncIOCompletion writeBlock(SequentialFile file,
                                              boolean syncWrites,
                                              final ByteBuffer bufferBlock,
                                              Supplier<? extends SyncIOCompletion> ioCallbackFactory) throws Exception {
      //reset it outside any latency measurement:
      //it is safe to do it with ASYNCIO and !syncWrites too, because it isn't used concurrently
      bufferBlock.position(0);
      SyncIOCompletion lastIOCallback = ioCallbackFactory.get();
      file.writeDirect(bufferBlock, true, lastIOCallback);

      if (syncWrites) {
         lastIOCallback.waitCompletion();
         lastIOCallback = null;
      }
      return lastIOCallback;
   }

   /**
    * Execute System.gc().
    *
    * @return {@code true} if succeed, {@code false} otherwise
    */
   private static boolean awaitGC(PrintStream err) throws InterruptedException {
      final List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans().stream().filter(bean -> bean.getCollectionCount() != -1).collect(Collectors.toList());
      long lastGcCount = gcBeans.stream().mapToLong(GarbageCollectorMXBean::getCollectionCount).sum();
      System.runFinalization();
      System.gc();
      System.runFinalization();
      System.gc();
      final int MAX_WAIT_GC_MILLIS = 20_000;
      //no beans is available to get GC stats
      if (gcBeans.isEmpty()) {
         err.println("WARNING: No MXBean can report GC info: System.gc() invoked");
         TimeUnit.MILLISECONDS.sleep(MAX_WAIT_GC_MILLIS);
         return true;
      }

      boolean requestedGcHasHappened = false;
      final long MAX_WAIT_GC_NANOS = TimeUnit.MILLISECONDS.toNanos(MAX_WAIT_GC_MILLIS);
      final long WAIT_GC_MILLIS = 200;
      assert WAIT_GC_MILLIS < MAX_WAIT_GC_MILLIS;
      long start = System.nanoTime();
      //wait until the number of GCs >= 2 from the 2 requests or the timeout has occurred
      while (!requestedGcHasHappened && (System.nanoTime() - start) < MAX_WAIT_GC_NANOS) {
         TimeUnit.MILLISECONDS.sleep(WAIT_GC_MILLIS);
         final long currentGcCount = gcBeans.stream().mapToLong(GarbageCollectorMXBean::getCollectionCount).sum();
         final long gcCount = currentGcCount - lastGcCount;
         if (gcCount >= 2) {
            requestedGcHasHappened = true;
            lastGcCount = currentGcCount;
         }
      }
      if (!requestedGcHasHappened) {
         //weird case
         err.println("System.gc() invoked but no GC has occurred yet");
         return false;
      }
      //wait until the number of GC won't became stable
      while ((System.nanoTime() - start) < MAX_WAIT_GC_NANOS) {
         TimeUnit.MILLISECONDS.sleep(WAIT_GC_MILLIS);
         final long currentGcCount = gcBeans.stream().mapToLong(GarbageCollectorMXBean::getCollectionCount).sum();
         final long gcCount = currentGcCount - lastGcCount;
         if (gcCount == 0) {
            return true;
         }
         lastGcCount = currentGcCount;
      }
      //probably the application is doing other allocations in "background"
      err.println("System.gc() invoked but GCs are not stopped yet");
      return false;
   }
}
