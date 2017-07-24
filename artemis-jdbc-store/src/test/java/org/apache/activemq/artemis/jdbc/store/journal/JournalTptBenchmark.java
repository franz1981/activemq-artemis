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

package org.apache.activemq.artemis.jdbc.store.journal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Stream;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.core.journal.EncodingSupport;
import org.apache.activemq.artemis.core.journal.Journal;
import org.apache.activemq.artemis.jdbc.store.drivers.JDBCUtils;
import org.apache.activemq.artemis.jdbc.store.sql.SQLProvider;

public class JournalTptBenchmark {

   public static void main(String[] args) throws Exception {
      final int producers = 1;
      final String jdbcUrl = "jdbc:oracle:thin:system/oracle@127.0.0.1:1521:XE";
      final String jdbcDriverClass = "oracle.jdbc.driver.OracleDriver";
      final int tests = 1;
      final int warmup = 100;
      final int measurements = 100;
      final long totalMeasurementsPerProducer = tests * warmup * measurements;
      final int msgSize = 1000;
      final byte[] msgContent = new byte[msgSize];
      Arrays.fill(msgContent, (byte) 1);
      final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(5);
      final ExecutorService executorService = Executors.newFixedThreadPool(5);
      final SQLProvider sqlProvider = JDBCUtils.getSQLProviderFactory(jdbcUrl).create("MESSAGE", SQLProvider.DatabaseStoreType.MESSAGE_JOURNAL);
      final Journal journal = new JDBCJournalImpl(jdbcUrl, jdbcDriverClass, sqlProvider, scheduledExecutorService, executorService, (code, message, file) -> System.err.println("code: " + code + "\tmessage: " + message + "\tfile: " + file));
      journal.start();
      try {
         journal.load(new ArrayList<>(), null, null);
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
      try {
         final EncodingSupport encodingSupport = new EncodingSupport() {
            @Override
            public int getEncodeSize() {
               return msgSize;
            }

            @Override
            public void encode(ActiveMQBuffer buffer) {
               final int writerIndex = buffer.writerIndex();
               buffer.setBytes(writerIndex, msgContent);
               buffer.writerIndex(writerIndex + msgSize);
            }

            @Override
            public void decode(ActiveMQBuffer buffer) {

            }
         };
         final CountDownLatch warmUpLatch = new CountDownLatch(producers * 2);
         final CountDownLatch[] testsLatch = new CountDownLatch[tests];
         for (int i = 0; i < tests; i++) {
            testsLatch[i] = new CountDownLatch(producers * 2);
         }
         final Thread[] producersTasks = new Thread[producers * 2];
         final AtomicLong[] idsProduced = new AtomicLong[producers];
         for (int i = 0; i < producers * 2; i++) {
            if (i % 2 == 0) {
               final long currentId = totalMeasurementsPerProducer * (i / 2);
               final AtomicLong trackedId = new AtomicLong(-1);
               idsProduced[i / 2] = trackedId;
               //producer
               producersTasks[i] = new Thread(() -> {
                  long id = currentId;
                  try {
                     {
                        warmUpLatch.countDown();
                        warmUpLatch.await();
                        final long elapsed = produceMeasurements(id, trackedId, journal, encodingSupport, warmup);
                        id += warmup;
                        System.out.println("WARMUP @ [" + Thread.currentThread() + "] - " + (warmup * 1000_000_000L) / elapsed + " ops/sec");
                     }

                     for (int t = 0; t < tests; t++) {
                        testsLatch[t].countDown();
                        testsLatch[t].await();
                        final long elapsed = produceMeasurements(id, trackedId, journal, encodingSupport, measurements);
                        id += measurements;
                        System.out.println((t + 1) + " @ [" + Thread.currentThread() + "] - " + ((measurements * 1000_000_000L) / elapsed) + " ops/sec");
                     }
                  } catch (Throwable e) {
                     e.printStackTrace();
                  }
               });
            } else {
               final long currentId = totalMeasurementsPerProducer * ((i - 1) / 2);
               final AtomicLong trackedId = idsProduced[(i - 1) / 2];
               //consumer
               producersTasks[i] = new Thread(() -> {
                  long id = currentId;
                  try {
                     {
                        warmUpLatch.countDown();
                        warmUpLatch.await();
                        final long elapsed = consumeMeasurements(id, trackedId, journal, warmup);
                        id += warmup;
                        System.out.println("CONSUMER WARMUP @ [" + Thread.currentThread() + "] - " + (warmup * 1000_000_000L) / elapsed + " ops/sec");
                     }

                     for (int t = 0; t < tests; t++) {
                        testsLatch[t].countDown();
                        testsLatch[t].await();
                        final long elapsed = consumeMeasurements(id, trackedId, journal, measurements);
                        id += measurements;
                        System.out.println("CONSUMER " + (t + 1) + " @ [" + Thread.currentThread() + "] - " + ((measurements * 1000_000_000L) / elapsed) + " ops/sec");
                     }
                  } catch (Throwable e) {
                     e.printStackTrace();
                  }
               });
            }
         }
         Stream.of(producersTasks).forEach(Thread::start);
         Stream.of(producersTasks).forEach(t -> {
            try {
               t.join();
            } catch (Throwable tr) {
               tr.printStackTrace();
            }
         });
      } finally {
         journal.stop();
         executorService.shutdown();
         scheduledExecutorService.shutdown();
      }
   }

   private static long produceMeasurements(long id,
                                           AtomicLong trackedId,
                                           Journal journal,
                                           EncodingSupport encodingSupport,
                                           int measurements) throws Exception {
      TimeUnit.SECONDS.sleep(2);
      final long start = System.nanoTime();
      for (int i = 0; i < measurements; i++) {
         journal.appendAddRecord(id, (byte) 1, encodingSupport, true);
         trackedId.lazySet(id);
         id++;
      }
      final long elapsed = System.nanoTime() - start;
      return elapsed;
   }

   private static long consumeMeasurements(long id,
                                           AtomicLong trackedId,
                                           Journal journal,
                                           int measurements) throws Exception {
      TimeUnit.SECONDS.sleep(2);
      final long start = System.nanoTime();
      for (int i = 0; i < measurements; i++) {
         //wait until id is produced
         while (trackedId.get() < id) {
            LockSupport.parkNanos(1L);
         }
         journal.appendDeleteRecord(id, true);
         id++;
      }

      final long elapsed = System.nanoTime() - start;
      return elapsed;
   }
}