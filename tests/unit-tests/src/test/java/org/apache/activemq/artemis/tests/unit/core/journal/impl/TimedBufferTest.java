/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.unit.core.journal.impl;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.io.buffer.TimedBuffer;
import org.apache.activemq.artemis.core.io.buffer.TimedBufferObserver;
import org.apache.activemq.artemis.core.journal.EncodingSupport;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Assert;
import org.junit.Test;

public class TimedBufferTest extends ActiveMQTestBase {

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private static final int ONE_SECOND_IN_NANOS = 1000000000; // in nanoseconds

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   IOCallback dummyCallback = new IOCallback() {

      @Override
      public void done() {
      }

      @Override
      public void onError(final int errorCode, final String errorMessage) {
      }
   };

   @Test
   public void testFillBuffer() {
      final ArrayList<ByteBuffer> buffers = new ArrayList<>();
      final AtomicInteger flushTimes = new AtomicInteger(0);
      class TestObserver implements TimedBufferObserver {

         @Override
         public void flushBuffer(final ByteBuffer buffer, final boolean sync, final List<IOCallback> callbacks) {
            buffers.add(buffer);
            flushTimes.incrementAndGet();
         }

         /* (non-Javadoc)
          * @see org.apache.activemq.artemis.utils.timedbuffer.TimedBufferObserver#newBuffer(int, int)
          */
         @Override
         public ByteBuffer newBuffer(final int minSize, final int maxSize) {
            return ByteBuffer.allocate(maxSize);
         }

         @Override
         public int getRemainingBytes() {
            return 1024 * 1024;
         }
      }

      TimedBuffer timedBuffer = new TimedBuffer(100, TimedBufferTest.ONE_SECOND_IN_NANOS, false);

      timedBuffer.start();

      try {

         timedBuffer.setObserver(new TestObserver());

         int x = 0;
         for (int i = 0; i < 10; i++) {
            byte[] bytes = new byte[10];
            for (int j = 0; j < 10; j++) {
               bytes[j] = ActiveMQTestBase.getSamplebyte(x++);
            }

            ActiveMQBuffer buff = ActiveMQBuffers.wrappedBuffer(bytes);

            timedBuffer.checkSize(10);
            timedBuffer.addBytes(buff, false, dummyCallback);
         }

         timedBuffer.checkSize(1);

         Assert.assertEquals(1, flushTimes.get());

         ByteBuffer flushedBuffer = buffers.get(0);

         Assert.assertEquals(100, flushedBuffer.limit());

         Assert.assertEquals(100, flushedBuffer.capacity());

         flushedBuffer.rewind();

         for (int i = 0; i < 100; i++) {
            Assert.assertEquals(ActiveMQTestBase.getSamplebyte(i), flushedBuffer.get());
         }
      } finally {
         timedBuffer.stop();
      }

   }

   private static void flushThatTooks(long deadLine) {
      while (System.nanoTime() < deadLine) {
         //noop
      }
   }

   @Test
   public void shouldNotFlushUntilTimeout() throws InterruptedException {
      final int bufferSize = 8;

      //100 milliseconds of timeout
      final int timeoutInNanos = (int) TimeUnit.MILLISECONDS.toNanos(100);
      final int fixedFlushTime = timeoutInNanos;
      final int messages = 10;
      final List<String> errors = new ArrayList<>(messages);

      class TestObserver implements TimedBufferObserver {

         private final ByteBuffer buffer = ByteBuffer.allocate(bufferSize);

         //safe start
         private long lastflushTime = System.nanoTime() - (timeoutInNanos + 1);

         @Override
         public void flushBuffer(final ByteBuffer buffer, final boolean sync, final List<IOCallback> callbacks) {
            //test valid only when sync is called!
            assert sync;
            //fake the consume of the buffer
            buffer.flip();
            final int writtenSize = buffer.limit();
            if (writtenSize > 0) {
               //only real writes will be considered IO
               final long now = System.nanoTime();
               //THE NEW FLUSH CAN'T BE REQUESTED BEFORE TIMEOUT HAS BEEN EXPIRED!!!
               final long interval = now - this.lastflushTime;
               if (interval <= timeoutInNanos) {
                  //not synchronized but uses callbacks::done to publish the values between threads
                  errors.add("A flush has happened before the expected timeout! BROKEN IOPS LIMIT");
               }
               //a successful flush will take at least timeoutInNanos (optimistic) -> in a real disk depends at least on the size!!!
               //spin wait to be precise :P
               final long expectedFlushDeadline = now + timeoutInNanos;
               flushThatTooks(expectedFlushDeadline);
               //take the real one
               this.lastflushTime = System.nanoTime();
               //test not OS interruptions
               assert lastflushTime >= expectedFlushDeadline;
               buffer.clear();
               callbacks.forEach(IOCallback::done);
            }
         }

         /* (non-Javadoc)
          * @see org.apache.activemq.artemis.utils.timedbuffer.TimedBufferObserver#newBuffer(int, int)
          */
         @Override
         public ByteBuffer newBuffer(final int maxSize, final int limit) {
            buffer.limit();
            return buffer;
         }

         //infinite file: we are optimist
         @Override
         public int getRemainingBytes() {
            return Integer.MAX_VALUE;
         }
      }

      final AtomicLong flushesDone = new AtomicLong(0);

      final IOCallback counterCallback = new IOCallback() {

         private long done = 0;

         @Override
         public void done() {
            this.done++;
            flushesDone.lazySet(this.done);
         }

         @Override
         public void onError(int errorCode, String errorMessage) {

         }
      };

      final TimedBuffer timedBuffer = new TimedBuffer(bufferSize, timeoutInNanos, false);
      timedBuffer.start();
      //need to wait the flusher thread to start to not invalidate the test!
      Thread.sleep(2_000);
      try {
         final EncodingSupport encodingSupport = new EncodingSupport() {

            private final int encodedSize = bufferSize;

            @Override
            public int getEncodeSize() {
               return bufferSize;
            }

            @Override
            public void encode(ActiveMQBuffer buffer) {
               //fake write, move only the write index
               buffer.writerIndex(buffer.writerIndex() + encodedSize);
            }

            @Override
            public void decode(ActiveMQBuffer buffer) {
               throw new UnsupportedOperationException("impossible in this test!");
            }
         };
         final int encodedSize = encodingSupport.getEncodeSize();
         timedBuffer.setObserver(new TestObserver());
         //the whole test should take more than...
         final int expectedMinTestTime = messages * (2 * timeoutInNanos);
         final long startTest = System.nanoTime();
         for (int i = 0; i < messages; i++) {
            final boolean canAddBytes = timedBuffer.checkSize(encodedSize);
            assert canAddBytes;
            timedBuffer.addBytes(encodingSupport, true, counterCallback);
         }
         //wait the last effective flush
         while (flushesDone.get() != messages) {
            //spin wait until the last is done
         }
         final long elapsedTestTime = System.nanoTime() - startTest;
         Assert.assertTrue("BROKEN IOPS LIMIT " + errors.size() + " TIMES!", errors.isEmpty());
         Assert.assertTrue("The test can't finish so early!", elapsedTestTime >= expectedMinTestTime);
      } finally {
         try {
            timedBuffer.stop();
         } catch (Throwable t) {
            //discard any exception here to not mess with test exception logs
         }
      }

   }

   @Test
   public void testTimingAndFlush() throws Exception {
      final ArrayList<ByteBuffer> buffers = new ArrayList<>();
      final AtomicInteger flushTimes = new AtomicInteger(0);
      class TestObserver implements TimedBufferObserver {

         @Override
         public void flushBuffer(final ByteBuffer buffer, final boolean sync, final List<IOCallback> callbacks) {
            buffers.add(buffer);
            flushTimes.incrementAndGet();
         }

         /* (non-Javadoc)
          * @see org.apache.activemq.artemis.utils.timedbuffer.TimedBufferObserver#newBuffer(int, int)
          */
         @Override
         public ByteBuffer newBuffer(final int minSize, final int maxSize) {
            return ByteBuffer.allocate(maxSize);
         }

         @Override
         public int getRemainingBytes() {
            return 1024 * 1024;
         }
      }

      TimedBuffer timedBuffer = new TimedBuffer(100, TimedBufferTest.ONE_SECOND_IN_NANOS / 10, false);

      timedBuffer.start();

      try {

         timedBuffer.setObserver(new TestObserver());

         int x = 0;

         byte[] bytes = new byte[10];
         for (int j = 0; j < 10; j++) {
            bytes[j] = ActiveMQTestBase.getSamplebyte(x++);
         }

         ActiveMQBuffer buff = ActiveMQBuffers.wrappedBuffer(bytes);

         timedBuffer.checkSize(10);
         timedBuffer.addBytes(buff, false, dummyCallback);

         Thread.sleep(200);

         Assert.assertEquals(0, flushTimes.get());

         bytes = new byte[10];
         for (int j = 0; j < 10; j++) {
            bytes[j] = ActiveMQTestBase.getSamplebyte(x++);
         }

         buff = ActiveMQBuffers.wrappedBuffer(bytes);

         timedBuffer.checkSize(10);
         timedBuffer.addBytes(buff, true, dummyCallback);

         Thread.sleep(500);

         Assert.assertEquals(1, flushTimes.get());

         ByteBuffer flushedBuffer = buffers.get(0);

         Assert.assertEquals(20, flushedBuffer.limit());

         Assert.assertEquals(20, flushedBuffer.capacity());

         flushedBuffer.rewind();

         for (int i = 0; i < 20; i++) {
            Assert.assertEquals(ActiveMQTestBase.getSamplebyte(i), flushedBuffer.get());
         }
      } finally {
         timedBuffer.stop();
      }

   }

   /**
    * This test will verify if the system will switch to spin case the system can't perform sleeps timely
    * due to proper kernel installations
    *
    * @throws Exception
    */
   @Test
   public void testVerifySwitchToSpin() throws Exception {
      class TestObserver implements TimedBufferObserver {

         @Override
         public void flushBuffer(final ByteBuffer buffer, final boolean sync, final List<IOCallback> callbacks) {
         }

         /* (non-Javadoc)
          * @see org.apache.activemq.artemis.utils.timedbuffer.TimedBufferObserver#newBuffer(int, int)
          */
         @Override
         public ByteBuffer newBuffer(final int minSize, final int maxSize) {
            return ByteBuffer.allocate(maxSize);
         }

         @Override
         public int getRemainingBytes() {
            return 1024 * 1024;
         }
      }

      final CountDownLatch sleptLatch = new CountDownLatch(1);

      TimedBuffer timedBuffer = new TimedBuffer(100, TimedBufferTest.ONE_SECOND_IN_NANOS / 1000, false) {

         @Override
         protected void stopSpin() {
            // keeps spinning forever
         }

         @Override
         protected void sleep(int sleepMillis, int sleepNanos) throws InterruptedException {
            Thread.sleep(10);
         }

         @Override
         public synchronized void setUseSleep(boolean param) {
            super.setUseSleep(param);
            sleptLatch.countDown();
         }

      };

      timedBuffer.start();

      try {

         timedBuffer.setObserver(new TestObserver());

         int x = 0;

         byte[] bytes = new byte[10];
         for (int j = 0; j < 10; j++) {
            bytes[j] = ActiveMQTestBase.getSamplebyte(x++);
         }

         ActiveMQBuffer buff = ActiveMQBuffers.wrappedBuffer(bytes);

         timedBuffer.checkSize(10);
         timedBuffer.addBytes(buff, true, dummyCallback);

         sleptLatch.await(10, TimeUnit.SECONDS);

         assertFalse(timedBuffer.isUseSleep());
      } finally {
         timedBuffer.stop();
      }

   }

   /**
    * This test will verify if the system will switch to spin case the system can't perform sleeps timely
    * due to proper kernel installations
    *
    * @throws Exception
    */
   @Test
   public void testStillSleeps() throws Exception {
      class TestObserver implements TimedBufferObserver {

         @Override
         public void flushBuffer(final ByteBuffer buffer, final boolean sync, final List<IOCallback> callbacks) {
         }

         /* (non-Javadoc)
          * @see org.apache.activemq.artemis.utils.timedbuffer.TimedBufferObserver#newBuffer(int, int)
          */
         @Override
         public ByteBuffer newBuffer(final int minSize, final int maxSize) {
            return ByteBuffer.allocate(maxSize);
         }

         @Override
         public int getRemainingBytes() {
            return 1024 * 1024;
         }
      }

      final CountDownLatch sleptLatch = new CountDownLatch(TimedBuffer.MAX_CHECKS_ON_SLEEP);

      TimedBuffer timedBuffer = new TimedBuffer(100, TimedBufferTest.ONE_SECOND_IN_NANOS / 1000, false) {

         @Override
         protected void stopSpin() {
            // keeps spinning forever
         }

         @Override
         protected void sleep(int sleepMillis, int sleepNanos) throws InterruptedException {
            sleptLatch.countDown();
            // no sleep
         }

         @Override
         public synchronized void setUseSleep(boolean param) {
            super.setUseSleep(param);
            sleptLatch.countDown();
         }

      };

      timedBuffer.start();

      try {

         timedBuffer.setObserver(new TestObserver());

         int x = 0;

         byte[] bytes = new byte[10];
         for (int j = 0; j < 10; j++) {
            bytes[j] = ActiveMQTestBase.getSamplebyte(x++);
         }

         ActiveMQBuffer buff = ActiveMQBuffers.wrappedBuffer(bytes);

         timedBuffer.checkSize(10);
         timedBuffer.addBytes(buff, true, dummyCallback);

         // waits all the sleeps to be done
         sleptLatch.await(10, TimeUnit.SECONDS);

         // keeps waiting a bit longer
         Thread.sleep(100);

         assertTrue(timedBuffer.isUseSleep());
      } finally {
         timedBuffer.stop();
      }
   }
}
