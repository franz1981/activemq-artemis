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

package org.apache.activemq.artemis.core.io.buffer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import io.netty.buffer.Unpooled;
import org.HdrHistogram.Histogram;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQExceptionType;
import org.apache.activemq.artemis.core.buffers.impl.ChannelBufferWrapper;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.journal.EncodingSupport;
import org.apache.activemq.artemis.core.journal.impl.SimpleWaitIOCallback;
import org.apache.activemq.artemis.journal.ActiveMQJournalLogger;
import org.apache.activemq.artemis.utils.critical.CriticalAnalyzer;
import org.apache.activemq.artemis.utils.critical.CriticalComponentImpl;

public final class BatchingBuffer extends CriticalComponentImpl implements WriteBuffer {

   private static final class Operation {

      private enum Type {
         SetObserver, AddBytes, Flush, CheckSize
      }

      private Type type;
      ChannelBufferWrapper writtenBytes;
      IOCallback ioCallback;
      boolean requiredSync;
      TimedBufferObserver observer;

      private void reset() {
         this.type = null;
         this.ioCallback = null;
         this.requiredSync = false;
         this.observer = null;
         if (writtenBytes != null) {
            writtenBytes.clear();
         }
      }
   }

   private final class BatchFlusher implements EventHandler<Operation> {

      private final int maxBatchSize;
      private int currentBatchSize;
      private ByteBuffer batchBuffer;
      private final List<IOCallback> ioCallbacks;
      private boolean requiredSync;
      private TimedBufferObserver observer;

      BatchFlusher(int maxBatchSize) {
         this.maxBatchSize = maxBatchSize;
         this.currentBatchSize = 0;
         this.batchBuffer = null;
         this.ioCallbacks = new ArrayList<>();
         this.requiredSync = false;
         this.observer = null;
      }

      @Override
      public void onEvent(Operation event, long sequence, boolean endOfBatch) {
         try {
            switch (event.type) {
               case CheckSize:
                  onCheckSize(event);
                  break;
               case SetObserver:
                  onChangeObserver(event);
                  break;
               case AddBytes:
                  //addBytes
                  final ChannelBufferWrapper buffer = event.writtenBytes;
                  final int expectedWrite = buffer.readableBytes();
                  final boolean eventRequiredSync = event.requiredSync;
                  if (batchBuffer == null) {
                     allocateBatchBuffer(endOfBatch, eventRequiredSync, expectedWrite);
                  } else if (expectedWrite > batchBuffer.remaining()) {
                     //need to flush the current batch to allow further writes
                     flushBatch();
                     if (batchBuffer == null) {
                        allocateBatchBuffer(endOfBatch, eventRequiredSync, expectedWrite);
                     }
                  }
                  assert expectedWrite <= batchBuffer.remaining();
                  final int batchLimit = batchBuffer.limit();
                  try {
                     final int startingPosition = batchBuffer.position();
                     batchBuffer.limit(startingPosition + expectedWrite);
                     buffer.getBytes(0, batchBuffer);
                  } finally {
                     final IOCallback eventIoCallback = event.ioCallback;
                     if (eventRequiredSync) {
                        requiredSync = true;
                     }
                     if (eventIoCallback != null) {
                        ioCallbacks.add(eventIoCallback);
                     }
                     currentBatchSize += expectedWrite;
                     //restore batch limit
                     batchBuffer.limit(batchLimit);
                  }
                  if ((endOfBatch && requiredSync) || !batchBuffer.hasRemaining()) {
                     flushBatch();
                  }
                  break;
               case Flush:
                  onSyncFlush(event);
                  break;
            }
         } finally {
            event.reset();
         }
      }

      private void onCheckSize(Operation event) {
         final int remainingSize;
         if (observer == null) {
            remainingSize = 0;
         } else {
            remainingSize = observer.getRemainingBytes();
         }
         expectedRemainingSize = Math.max(remainingSize - (int) align(currentBatchSize, writeAlignment), 0);
         event.ioCallback.done();
      }

      private void onChangeObserver(Operation event) {
         if (observer != null) {
            if (event.requiredSync) {
               requiredSync = true;
            }
            flushBatch();
         }
         //it could be null or not it doesn't matter
         observer = event.observer;
         if (event.ioCallback != null) {
            event.ioCallback.done();
         }
      }

      private void onSyncFlush(Operation event) {
         if (event.requiredSync) {
            requiredSync = true;
         }
         flushBatch();
         if (event.ioCallback != null) {
            event.ioCallback.done();
         }
      }

      private void allocateBatchBuffer(boolean endOfBatch, boolean requiredSync, int expectedWrite) {
         assert batchBuffer == null;
         final int bufferLimit;
         if (endOfBatch && requiredSync) {
            bufferLimit = expectedWrite;
         } else {
            bufferLimit = Math.max(maxBatchSize, expectedWrite);
         }
         final int requiredCapacity = Math.max(maxBatchSize, bufferLimit);
         batchBuffer = observer.newBuffer(requiredCapacity, bufferLimit);
         batchBuffer.limit(bufferLimit);
      }

      private void flushBatch() {
         if (this.currentBatchSize > 0) {
            try {
               final List<IOCallback> callbacks;
               if (!this.ioCallbacks.isEmpty()) {
                  callbacks = new ArrayList<>(this.ioCallbacks);
               } else {
                  callbacks = Collections.emptyList();
               }
               this.observer.flushBuffer(this.batchBuffer, requiredSync, callbacks);
            } finally {
               this.currentBatchSize = 0;
               this.batchBuffer = null;
               this.requiredSync = false;
               batchDistribution.recordValue(ioCallbacks.size());
               this.ioCallbacks.clear();
            }
         }
      }
   }

   protected static final int CRITICAL_PATHS = 6;
   protected static final int CRITICAL_PATH_FLUSH = 0;
   protected static final int CRITICAL_PATH_STOP = 1;
   protected static final int CRITICAL_PATH_START = 2;
   protected static final int CRITICAL_PATH_CHECK_SIZE = 3;
   protected static final int CRITICAL_PATH_ADD_BYTES = 4;
   protected static final int CRITICAL_PATH_SET_OBSERVER = 5;

   private Disruptor<Operation> disruptor;
   private RingBuffer<Operation> operations;
   private final int capacity;
   private final int batchSize;
   private int expectedRemainingSize;
   private final WaitStrategy waitStrategy;
   private final int writeAlignment;
   private final Histogram batchDistribution;

   public BatchingBuffer(CriticalAnalyzer analyzer,
                         int writeAlignment,
                         int capacity,
                         int batchSize,
                         WaitStrategy waitStrategy) {
      super(analyzer, CRITICAL_PATHS);
      this.capacity = capacity;
      this.batchSize = batchSize;
      this.expectedRemainingSize = 0;
      this.waitStrategy = waitStrategy;
      if (Integer.bitCount(writeAlignment) != 1) {
         throw new IllegalArgumentException("write alignment = " + writeAlignment + " should be a pow of 2!");
      }
      this.writeAlignment = writeAlignment;
      this.batchDistribution = new Histogram(1, capacity, 0);
   }

   //branch-less pow of 2 align
   private static long align(long value, int alignment) {
      return value + (long) (alignment - 1) & (long) (~(alignment - 1));
   }

   @Override
   public void start() {
      enterCritical(CRITICAL_PATH_START);
      try {
         synchronized (this) {
            if (this.disruptor == null) {
               this.batchDistribution.reset();
               this.disruptor = new Disruptor<>(Operation::new, capacity, r -> {
                  final Thread t = new Thread(r);
                  t.setName("activemq-batching-buffer");
                  return t;
               }, ProducerType.SINGLE, waitStrategy);
               this.disruptor.handleEventsWith(new BatchFlusher(this.batchSize));
               this.operations = this.disruptor.start();
            }
         }
      } finally {
         leaveCritical(CRITICAL_PATH_START);
      }
   }

   @Override
   public void stop() {
      enterCritical(CRITICAL_PATH_STOP);
      try {
         synchronized (this) {
            if (this.disruptor != null) {
               try {
                  this.disruptor.shutdown();
               } finally {
                  this.disruptor = null;
                  this.operations = null;
                  this.expectedRemainingSize = 0;
                  batchDistribution.outputPercentileDistribution(System.out, 1d);
               }
            }
         }
      } finally {
         leaveCritical(CRITICAL_PATH_STOP);
      }
   }

   @Override
   public void setObserver(TimedBufferObserver observer) {
      enterCritical(CRITICAL_PATH_SET_OBSERVER);
      try {
         synchronized (this) {
            final SimpleWaitIOCallback finished = new SimpleWaitIOCallback();
            if (observer != null) {
               //this observer isn't been used yet ie can be queried without fearing any races
               //if this contract is not honored would be better to use a Future<Integer> instead
               //of CountDownLatch and wait until the flusher thread will answer
               expectedRemainingSize = observer.getRemainingBytes();
            } else {
               expectedRemainingSize = 0;
            }
            final long next = this.operations.next();
            try {
               final Operation operation = this.operations.get(next);
               operation.type = Operation.Type.SetObserver;
               operation.observer = observer;
               operation.requiredSync = true;
               operation.ioCallback = finished;
            } finally {
               this.operations.publish(next);
            }
            try {
               finished.waitCompletion();
            } catch (InterruptedException e) {
               expectedRemainingSize = 0;
               //TODO
            } catch (ActiveMQException e) {
               //TODO
            }
         }
      } finally {
         leaveCritical(CRITICAL_PATH_SET_OBSERVER);
      }
   }

   private static final class SpinIOCallback implements IOCallback {

      private final AtomicBoolean finished = new AtomicBoolean();
      private int errorCode = 0;
      private String errorMessage = null;

      SpinIOCallback() {

      }

      @Override
      public void done() {
         finished.lazySet(true);
      }

      @Override
      public void onError(int errorCode, String errorMessage) {
         this.errorCode = errorCode;
         this.errorMessage = errorMessage;
         ActiveMQJournalLogger.LOGGER.errorOnIOCallback(errorMessage);
         finished.lazySet(false);
      }

      public SpinIOCallback reset() {
         errorCode = 0;
         errorMessage = null;
         finished.lazySet(false);
         return this;
      }

      public void waitCompletion() throws ActiveMQException, InterruptedException {
         while (!finished.get()) {
            LockSupport.parkNanos(1L);
            if (Thread.interrupted())
               throw new InterruptedException();
         }
         if (errorMessage != null) {
            throw ActiveMQExceptionType.createException(errorCode, errorMessage);
         }
      }
   }

   private final SpinIOCallback ioCallback = new SpinIOCallback();

   @Override
   public boolean checkSize(int sizeChecked) {
      enterCritical(CRITICAL_PATH_CHECK_SIZE);
      try {
         synchronized (this) {
            final int alignedSize = (int) align(sizeChecked, writeAlignment);
            if (alignedSize <= this.expectedRemainingSize) {
               return true;
            } else {
               //recalibrate expectedRemainingSize using a blocking call vs the current observer
               final SpinIOCallback finished = this.ioCallback.reset();
               final long next = this.operations.next();
               try {
                  final Operation operation = this.operations.get(next);
                  operation.type = Operation.Type.CheckSize;
                  operation.ioCallback = finished;
               } finally {
                  this.operations.publish(next);
               }
               try {
                  finished.waitCompletion();
                  //here expectedRemainingSize should be correctly updated
               } catch (InterruptedException e) {
                  //TODO
               } catch (ActiveMQException e) {
                  //TODO
               }
               return alignedSize <= this.expectedRemainingSize;
            }
         }
      } finally {
         leaveCritical(CRITICAL_PATH_CHECK_SIZE);
      }
   }

   @Override
   public void addBytes(ActiveMQBuffer bytes, boolean sync, IOCallback callback) {
      enterCritical(CRITICAL_PATH_ADD_BYTES);
      try {
         synchronized (this) {
            //it doesn't modify the reader index of bytes as in the original version
            final int readableBytes = bytes.readableBytes();
            final long next = this.operations.next();
            try {
               final Operation operation = this.operations.get(next);
               final ChannelBufferWrapper buffer;
               if (operation.writtenBytes == null || operation.writtenBytes.capacity() < readableBytes) {
                  buffer = new ChannelBufferWrapper(Unpooled.wrappedBuffer(ByteBuffer.allocateDirect(readableBytes)));
                  buffer.clear();
               } else {
                  buffer = operation.writtenBytes;
               }
               //any failures will create a null write: much safer than write anything wrong
               operation.type = Operation.Type.AddBytes;
               operation.writtenBytes = null;
               final int writerIndex = buffer.writerIndex();
               buffer.setBytes(writerIndex, bytes, bytes.readerIndex(), readableBytes);
               buffer.writerIndex(writerIndex + readableBytes);
               operation.writtenBytes = buffer;
               operation.ioCallback = callback;
               operation.requiredSync = sync;
               expectedRemainingSize -= align(readableBytes, writeAlignment);
            } finally {
               this.operations.publish(next);
            }
         }
      } finally {
         leaveCritical(CRITICAL_PATH_ADD_BYTES);
      }
   }

   @Override
   public void addBytes(EncodingSupport bytes, boolean sync, IOCallback callback) {
      enterCritical(CRITICAL_PATH_ADD_BYTES);
      try {
         synchronized (this) {
            //it doesn't modify the reader index of bytes as in the original version
            final int readableBytes = bytes.getEncodeSize();
            final long next = this.operations.next();
            try {
               final Operation operation = this.operations.get(next);
               final ChannelBufferWrapper buffer;
               if (operation.writtenBytes == null || operation.writtenBytes.capacity() < readableBytes) {
                  buffer = new ChannelBufferWrapper(Unpooled.wrappedBuffer(ByteBuffer.allocateDirect(readableBytes)));
                  buffer.clear();
               } else {
                  buffer = operation.writtenBytes;
               }
               //any failures will create a null write: much safer than write anything wrong
               operation.type = Operation.Type.AddBytes;
               operation.writtenBytes = null;
               bytes.encode(buffer);
               operation.writtenBytes = buffer;
               operation.ioCallback = callback;
               operation.requiredSync = sync;
               expectedRemainingSize -= align(readableBytes, writeAlignment);
            } finally {
               this.operations.publish(next);
            }
         }
      } finally {
         leaveCritical(CRITICAL_PATH_ADD_BYTES);
      }
   }

   @Override
   public void flush() {
      enterCritical(CRITICAL_PATH_FLUSH);
      try {
         synchronized (this) {
            final SimpleWaitIOCallback finished = new SimpleWaitIOCallback();
            final long next = this.operations.next();
            try {
               final Operation operation = this.operations.get(next);
               operation.type = Operation.Type.Flush;
               operation.ioCallback = finished;
               operation.requiredSync = true;
            } finally {
               this.operations.publish(next);
            }
            try {
               finished.waitCompletion();
            } catch (InterruptedException e) {
               //TODO
            } catch (ActiveMQException e) {
               //TODO
            }
         }
      } finally {
         leaveCritical(CRITICAL_PATH_FLUSH);
      }
   }

}
