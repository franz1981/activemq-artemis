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

package org.apache.activemq.artemis.core.io;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.apache.activemq.artemis.ArtemisConstants;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.core.io.aio.AIOSequentialFileFactory;
import org.apache.activemq.artemis.core.io.mapped.MappedSequentialFileFactory;
import org.apache.activemq.artemis.core.io.nio.NIOSequentialFileFactory;
import org.apache.activemq.artemis.core.journal.EncodingSupport;
import org.apache.activemq.artemis.core.journal.Journal;
import org.apache.activemq.artemis.core.journal.RecordInfo;
import org.apache.activemq.artemis.core.journal.impl.JournalImpl;
import org.apache.activemq.artemis.jlibaio.LibaioContext;

/**
 * To benchmark Type.Aio you need to define -Djava.library.path=/home/forked_franz/IdeaProjects/activemq-artemis/artemis-native/bin/ when calling the JVM
 */
public class JournalTptBenchmark {

   public static void main(String[] args) throws Exception {
      final int numFiles = 4;
      final int poolFiles = 4;
      final int producers = 2;
      final int synched = 2;
      final int fileSize = 10 * 1024 * 1024;
      final boolean dataSync = true;
      final Type type = Type.Mapped;
      final int tests = 1;
      final int warmup = 10000;
      final int measurements = 10000;
      final int msgSize = 1000;
      final int bufferTimeout = 1927999;
      final byte[] msgContent = new byte[msgSize];
      Arrays.fill(msgContent, (byte) 1);
      final File tmpDirectory = new File("./tmp");
      Files.createDirectory(tmpDirectory.toPath());
      tmpDirectory.deleteOnExit();
      //using the default configuration when the broker starts!
      final SequentialFileFactory factory;
      switch (type) {

         case Mapped:
            factory = MappedSequentialFileFactory.buffered(tmpDirectory, fileSize, ArtemisConstants.DEFAULT_JOURNAL_BUFFER_SIZE_AIO, bufferTimeout / 4, null).setDatasync(dataSync);
            break;
         case Nio:
            factory = new NIOSequentialFileFactory(tmpDirectory, true, ArtemisConstants.DEFAULT_JOURNAL_BUFFER_SIZE_AIO, bufferTimeout / 4, 1, false, null).setDatasync(dataSync);
            break;
         case Aio:
            factory = new AIOSequentialFileFactory(tmpDirectory, ArtemisConstants.DEFAULT_JOURNAL_BUFFER_SIZE_AIO, bufferTimeout, 500, false, null).setDatasync(dataSync);
            //disable it when using directly the same buffer: ((AIOSequentialFileFactory)factory).disableBufferReuse();
            if (!LibaioContext.isLoaded()) {
               throw new IllegalStateException("lib AIO not loaded!");
            }
            break;
         default:
            throw new AssertionError("unsupported case");
      }
      ExecutorService service = null;
      final Journal journal = new JournalImpl(fileSize, numFiles, poolFiles, Integer.MAX_VALUE, 100, factory, "activemq-data", "amq", factory.getMaxIO());
      journal.start();
      try {
         journal.load(new ArrayList<RecordInfo>(), null, null);
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
         final AtomicLong idGenerator = new AtomicLong(0);
         final CountDownLatch warmUpLatch = new CountDownLatch(producers);
         final CountDownLatch[] testsLatch = new CountDownLatch[tests];
         for (int i = 0; i < tests; i++) {
            testsLatch[i] = new CountDownLatch(producers);
         }
         final Thread[] producersTasks = new Thread[producers];
         int syncCount = synched;
         for (int i = 0; i < producers; i++) {
            final boolean sync = syncCount > 0;
            syncCount--;
            producersTasks[i] = new Thread(() -> {
               try {
                  {
                     warmUpLatch.countDown();
                     warmUpLatch.await();
                     final long elapsed = writeMeasurements(idGenerator, journal, encodingSupport, warmup, sync);
                     System.out.println((sync ? "SYNC - " : "") + "WARMUP @ [" + Thread.currentThread() + "] - " + (warmup * 1000_000_000L) / elapsed + " ops/sec");
                  }

                  for (int t = 0; t < tests; t++) {
                     testsLatch[t].countDown();
                     testsLatch[t].await();
                     final long elapsed = writeMeasurements(idGenerator, journal, encodingSupport, measurements, sync);
                     System.out.println((sync ? "SYNC - " : "") + (t + 1) + " @ [" + Thread.currentThread() + "] - " + ((measurements * 1000_000_000L) / elapsed) + " ops/sec");
                  }
               } catch (Throwable e) {
                  e.printStackTrace();
               }
            });
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
         if (service != null) {
            service.shutdown();
         }
         final File[] fileToDeletes = tmpDirectory.listFiles();
         System.out.println("Files to deletes" + Arrays.toString(fileToDeletes));
         Stream.of(fileToDeletes).forEach(File::delete);
      }
   }

   private static long writeMeasurements(AtomicLong id,
                                         Journal journal,
                                         EncodingSupport encodingSupport,
                                         int measurements,
                                         boolean sync) throws Exception {
      TimeUnit.SECONDS.sleep(2);
      long lastId = -1;
      final long start = System.nanoTime();
      for (int i = 0; i < measurements; i++) {
         if (lastId < 0) {
            lastId = id.getAndIncrement();
            journal.appendAddRecord(lastId, (byte) 1, encodingSupport, sync);
         } else {
            journal.appendDeleteRecord(lastId, sync);
            lastId = -1;
         }
      }
      final long elapsed = System.nanoTime() - start;
      return elapsed;
   }

   private enum Type {

      Mapped, Nio, Aio

   }
}