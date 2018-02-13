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

package org.apache.activemq.artemis.jlibaio.test;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.lmax.disruptor.LiteBlockingWaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;
import org.apache.activemq.artemis.jlibaio.SubmitInfo;
import org.junit.Assert;

public class RotatingFileAppenderTest {

   private static final class SubmitInfoCounter implements SubmitInfo {

      private final CyclicBarrier cyclicBarrier = new CyclicBarrier(2);

      @Override
      public void onError(int errno, String message) {
         Assert.fail("NO IO ERRORS ARE CONSIDERED HERE!");
      }

      @Override
      public void done() {
         try {
            cyclicBarrier.await();
         } catch (Throwable e) {
            e.printStackTrace();
         }
      }
   }

   /**
    * -Djava.library.path=./bin
    */
   public static void main(String[] args) throws InterruptedException, BrokenBarrierException {
      final int producers = 1;
      final File journalDir = new File("./journalTests");
      final ExecutorService producerTasks = Executors.newFixedThreadPool(producers);
      journalDir.mkdir();
      journalDir.deleteOnExit();
      final ThreadFactory factory = Thread::new;
      final boolean fillBeforeWrite = true;
      final int tests = 5;
      final int messages = 1000;
      final boolean fsync = true;
      final int fileSize = 100 * 1024 * 1024;
      final int messageSize = 4096;
      final int maxIO = 512;
      try (RotatingFileAppender rotatingFileAppender = new RotatingFileAppender(journalDir, fileSize, maxIO, factory, producers > 1 ? ProducerType.MULTI : ProducerType.SINGLE, new LiteBlockingWaitStrategy(), fillBeforeWrite, fsync)) {
         rotatingFileAppender.start();
         final CountDownLatch finished = new CountDownLatch(producers);
         final CyclicBarrier producersBarrier = new CyclicBarrier(producers);
         for (int p = 0; p < producers; p++) {
            producerTasks.execute(() -> {
               try {
                  final ByteBuffer msg = ByteBuffer.allocateDirect(messageSize).order(ByteOrder.nativeOrder());
                  producersBarrier.await();
                  final long msgId = Thread.currentThread().getId();
                  System.out.println("STARTED [ " + msgId + " ]");
                  final SubmitInfoCounter counter = new SubmitInfoCounter();
                  for (int t = 0; t < tests; t++) {
                     final long start = System.nanoTime();
                     for (int m = 0; m < messages; m++) {
                        msg.clear();
                        msg.putLong(0, msgId);
                        msg.position(messageSize);
                        rotatingFileAppender.append(msg, fsync, counter);
                        counter.cyclicBarrier.await();
                     }
                     final long elapsed = System.nanoTime() - start;
                     System.out.println("[ " + msgId + " ] - " + (messages * 1000_000_000L) / elapsed + " msg/sec");
                     producersBarrier.await();
                  }
               } catch (Throwable e) {
                  e.printStackTrace();
               } finally {
                  finished.countDown();
               }
            });
         }
         producerTasks.shutdown();
         finished.await();
      }
      int files = 0;
      for (File file : journalDir.listFiles()) {
         file.deleteOnExit();
         file.delete();
         files++;
      }
      System.out.println("deleted " + files + " journal files");
   }
}
