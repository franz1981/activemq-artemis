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
package org.apache.activemq.artemis.core.io.buffer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import io.netty.buffer.Unpooled;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQInterruptedException;
import org.apache.activemq.artemis.core.buffers.impl.ChannelBufferWrapper;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.journal.EncodingSupport;
import org.apache.activemq.artemis.journal.ActiveMQJournalLogger;
import org.apache.activemq.artemis.utils.critical.CriticalAnalyzer;
import org.apache.activemq.artemis.utils.critical.CriticalComponentImpl;

public final class TimedBuffer extends CriticalComponentImpl {

   protected static final int CRITICAL_PATHS = 6;
   protected static final int CRITICAL_PATH_FLUSH = 0;
   protected static final int CRITICAL_PATH_STOP = 1;
   protected static final int CRITICAL_PATH_START = 2;
   protected static final int CRITICAL_PATH_CHECK_SIZE = 3;
   protected static final int CRITICAL_PATH_ADD_BYTES = 4;
   protected static final int CRITICAL_PATH_SET_OBSERVER = 5;

   private TimedBufferObserver bufferObserver;

   private CheckTimer timerRunnable;

   private final int bufferSize;

   private final ActiveMQBuffer buffer;

   private int bufferLimit = 0;

   private List<IOCallback> callbacks;

   private final int timeout;

   // used to measure sync requests. When a sync is requested, it shouldn't take more than timeout to happen
   private volatile boolean pendingSync = false;

   private Thread timerThread;

   private volatile boolean started;

   // We use this flag to prevent flush occurring between calling checkSize and addBytes
   // CheckSize must always be followed by it's corresponding addBytes otherwise the buffer
   // can get in an inconsistent state
   private boolean delayFlush;

   // for logging write rates

   private final boolean logRates;

   private final AtomicInteger bufferLock = new AtomicInteger(UNLOCKED);
   private static final int UNLOCKED = 0;
   private static final int LOCKED_BUFFER = 1;
   private volatile Thread sleepingThread = null;

   private final AtomicLong bytesFlushed = new AtomicLong(0);

   private final AtomicLong flushesDone = new AtomicLong(0);

   private Timer logRatesTimer;

   private TimerTask logRatesTimerTask;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public TimedBuffer(CriticalAnalyzer analyzer, final int size, final int timeout, final boolean logRates) {
      super(analyzer, CRITICAL_PATHS);
      bufferSize = size;

      this.logRates = logRates;

      if (logRates) {
         logRatesTimer = new Timer(true);
      }
      // Setting the interval for nano-sleeps

      //prefer off heap buffer to allow further humongous allocations and reduce GC overhead
      //NOTE: it is used ByteBuffer::allocateDirect instead of Unpooled::directBuffer, because the latter could allocate
      //direct ByteBuffers with no Cleaner!
      buffer = new ChannelBufferWrapper(Unpooled.wrappedBuffer(ByteBuffer.allocateDirect(size)));

      buffer.clear();

      bufferLimit = 0;

      callbacks = new ArrayList<>();

      this.timeout = timeout;
   }

   public void start() {
      enterCritical(CRITICAL_PATH_START);
      try {
         lockBuffer();
         try {
            if (started) {
               return;
            }

            timerRunnable = new CheckTimer();

            timerThread = new Thread(timerRunnable, "activemq-buffer-timeout");

            timerThread.start();

            if (logRates) {
               logRatesTimerTask = new LogRatesTimerTask();

               logRatesTimer.scheduleAtFixedRate(logRatesTimerTask, 2000, 2000);
            }

            started = true;
         } finally {
            unlockBuffer();
         }
      } finally {
         leaveCritical(CRITICAL_PATH_START);
      }
   }

   private void lockBuffer() {
      final int MAX_SPINS = 10;
      int spins = 0;
      final AtomicInteger bufferLock = this.bufferLock;
      while (true) {
         if (bufferLock.compareAndSet(UNLOCKED, LOCKED_BUFFER)) {
            break;
         }
         if (spins < MAX_SPINS) {
            spins++;
         } else {
            Thread.yield();
         }
      }
   }

   private void unlockBuffer() {
      assert bufferLock.get() == LOCKED_BUFFER;
      bufferLock.lazySet(UNLOCKED);
   }

   public void stop() {
      enterCritical(CRITICAL_PATH_STOP);
      Thread localTimer = null;
      try {
         // add critical analyzer here.... <<<<
         lockBuffer();
         try {
            try {
               if (!started) {
                  return;
               }

               flushBatch();

               bufferObserver = null;

               timerRunnable.close();

               final Thread sleepingThread = this.sleepingThread;
               if (sleepingThread != null) {
                  LockSupport.unpark(sleepingThread);
               }

               if (logRates) {
                  logRatesTimerTask.cancel();
               }

               localTimer = timerThread;
               timerThread = null;

            } finally {
               started = false;
            }
         } finally {
            unlockBuffer();
         }
         if (localTimer != null) {
            while (localTimer.isAlive()) {
               try {
                  localTimer.join(1000);
                  if (localTimer.isAlive()) {
                     localTimer.interrupt();
                  }
               } catch (InterruptedException e) {
                  throw new ActiveMQInterruptedException(e);
               }
            }
         }
      } finally {
         leaveCritical(CRITICAL_PATH_STOP);
      }
   }

   public void setObserver(final TimedBufferObserver observer) {
      enterCritical(CRITICAL_PATH_SET_OBSERVER);
      try {
         lockBuffer();
         try {
            if (bufferObserver != null) {
               flushBatch();
            }

            bufferObserver = observer;
         } finally {
            unlockBuffer();
         }
      } finally {
         leaveCritical(CRITICAL_PATH_SET_OBSERVER);
      }
   }

   /**
    * Verify if the size fits the buffer
    *
    * @param sizeChecked
    */
   public boolean checkSize(final int sizeChecked) {
      enterCritical(CRITICAL_PATH_CHECK_SIZE);
      try {
         lockBuffer();
         try {
            if (!started) {
               throw new IllegalStateException("TimedBuffer is not started");
            }

            if (sizeChecked > bufferSize) {
               throw new IllegalStateException("Can't write records bigger than the bufferSize(" + bufferSize + ") on the journal");
            }

            if (bufferLimit == 0 || buffer.writerIndex() + sizeChecked > bufferLimit) {
               // Either there is not enough space left in the buffer for the sized record
               // Or a flush has just been performed and we need to re-calculate bufferLimit

               flushBatch();

               delayFlush = true;

               final int remainingInFile = bufferObserver.getRemainingBytes();

               if (sizeChecked > remainingInFile) {
                  return false;
               } else {
                  // There is enough space in the file for this size

                  // Need to re-calculate buffer limit

                  bufferLimit = Math.min(remainingInFile, bufferSize);

                  return true;
               }
            } else {
               delayFlush = true;

               return true;
            }
         } finally {
            unlockBuffer();
         }
      } finally {
         leaveCritical(CRITICAL_PATH_CHECK_SIZE);
      }
   }

   public void addBytes(final ActiveMQBuffer bytes, final boolean sync, final IOCallback callback) {
      enterCritical(CRITICAL_PATH_ADD_BYTES);
      try {
         lockBuffer();
         try {
            if (!started) {
               throw new IllegalStateException("TimedBuffer is not started");
            }

            delayFlush = false;

            //it doesn't modify the reader index of bytes as in the original version
            final int readableBytes = bytes.readableBytes();
            final int writerIndex = buffer.writerIndex();
            buffer.setBytes(writerIndex, bytes, bytes.readerIndex(), readableBytes);
            buffer.writerIndex(writerIndex + readableBytes);

            callbacks.add(callback);

            if (sync) {
               pendingSync = true;
               wakeupFlusherIfNeeded();
            }
         } finally {
            unlockBuffer();
         }
      } finally {
         leaveCritical(CRITICAL_PATH_ADD_BYTES);
      }
   }

   private void wakeupFlusherIfNeeded() {
      assert bufferLock.get() == LOCKED_BUFFER && pendingSync;
      final Thread sleeping = sleepingThread;
      if (sleeping != null) {
         LockSupport.unpark(sleeping);
      }
   }

   public void addBytes(final EncodingSupport bytes, final boolean sync, final IOCallback callback) {
      enterCritical(CRITICAL_PATH_ADD_BYTES);
      try {
         lockBuffer();
         try {
            if (!started) {
               throw new IllegalStateException("TimedBuffer is not started");
            }

            delayFlush = false;

            bytes.encode(buffer);

            callbacks.add(callback);

            if (sync) {
               pendingSync = true;
               wakeupFlusherIfNeeded();
            }
         } finally {
            unlockBuffer();
         }
      } finally {
         leaveCritical(CRITICAL_PATH_ADD_BYTES);
      }

   }

   public boolean flush() {
      enterCritical(CRITICAL_PATH_FLUSH);
      try {
         lockBuffer();
         try {
            return flushBatch();
         } finally {
            unlockBuffer();
         }
      } finally {
         leaveCritical(CRITICAL_PATH_FLUSH);
      }
   }

   /**
    * Attempts to flush if {@code !delayFlush} and {@code buffer} is filled by any data.
    *
    * @return {@code true} when are flushed any bytes, {@code false} otherwise
    */
   private boolean flushBatch() {
      assert bufferLock.get() == LOCKED_BUFFER;
      if (!started) {
         throw new IllegalStateException("TimedBuffer is not started");
      }

      if (!delayFlush && buffer.writerIndex() > 0) {
         int pos = buffer.writerIndex();

         if (logRates) {
            bytesFlushed.addAndGet(pos);
         }

         final ByteBuffer bufferToFlush = bufferObserver.newBuffer(bufferSize, pos);
         //bufferObserver::newBuffer doesn't necessary return a buffer with limit == pos or limit == bufferSize!!
         bufferToFlush.limit(pos);
         //perform memcpy under the hood due to the off heap buffer
         buffer.getBytes(0, bufferToFlush);

         bufferObserver.flushBuffer(bufferToFlush, pendingSync, callbacks);

         pendingSync = false;

         // swap the instance as the previous callback list is being used asynchronously
         callbacks = new ArrayList<>();

         buffer.clear();

         bufferLimit = 0;

         flushesDone.incrementAndGet();

         return pos > 0;
      } else {
         return false;
      }
   }

   private class LogRatesTimerTask extends TimerTask {

      private boolean closed;

      private long lastExecution;

      private long lastBytesFlushed;

      private long lastFlushesDone;

      @Override
      public synchronized void run() {
         if (!closed) {
            long now = System.currentTimeMillis();

            long bytesF = bytesFlushed.get();
            long flushesD = flushesDone.get();

            if (lastExecution != 0) {
               double rate = 1000 * (double) (bytesF - lastBytesFlushed) / (now - lastExecution);
               ActiveMQJournalLogger.LOGGER.writeRate(rate, (long) (rate / (1024 * 1024)));
               double flushRate = 1000 * (double) (flushesD - lastFlushesDone) / (now - lastExecution);
               ActiveMQJournalLogger.LOGGER.flushRate(flushRate);
            }

            lastExecution = now;

            lastBytesFlushed = bytesF;

            lastFlushesDone = flushesD;
         }
      }

      @Override
      public synchronized boolean cancel() {
         closed = true;

         return super.cancel();
      }
   }

   private class CheckTimer implements Runnable {

      private volatile boolean closed = false;

      @Override
      public void run() {
         final Thread currentThread = Thread.currentThread();
         final long timeout = TimedBuffer.this.timeout;
         long lastFlushTime;
         while (!closed) {
            // We flush on the timer if there are pending syncs there and we've waited at least one
            // timeout since the time of the last flush.
            // Effectively flushing "resets" the timer
            // On the timeout verification, notice that we ignore the timeout check if we are using sleep

            if (pendingSync) {
               lastFlushTime = System.nanoTime();
               if (flush()) {
                  //it could wait until the timeout is expired
                  // example: Say the device took 20% of the time to write..
                  //          We only need to wait 80% more..
                  //          And if the device took more than that time, there's no need to wait at all.
                  final long deadLine = lastFlushTime + timeout;
                  sleepUntil(deadLine);
               }
            }

            //we could park here if there are no pendingSyncs
            if (!pendingSync) {
               sleepingThread = currentThread;
               try {
                  if (!pendingSync) {
                     LockSupport.park();
                     if (currentThread.isInterrupted()) {
                        throw new ActiveMQInterruptedException(new InterruptedException());
                     }
                  }
               } finally {
                  sleepingThread = null;
               }
            }
         }
      }

      public void close() {
         closed = true;
      }
   }

   private static final long MINIMUM_SLEEP_NANOS = TimeUnit.MILLISECONDS.toNanos(1);
   private static final long MINIMUM_YIELD_NANOS = TimeUnit.MICROSECONDS.toNanos(10);

   private static void sleepUntil(long deadLine) {
      while (true) {
         final long toSleep = deadLine - System.nanoTime();
         if (toSleep <= 0) {
            return;
         }
         if (toSleep > MINIMUM_SLEEP_NANOS) {
            //do not trust parks and let it sleep only half of the expected time
            LockSupport.parkNanos(toSleep >> 1);
         } else if (toSleep > MINIMUM_YIELD_NANOS) {
            Thread.yield();
         }
      }
   }

}