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
package org.apache.activemq.artemis.core.io.mapped.batch;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.concurrent.ringbuffer.RefRingBuffer;
import org.apache.activemq.artemis.concurrent.ringbuffer.RingBuffers;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.io.buffer.TimedBufferObserver;
import org.apache.activemq.artemis.core.io.buffer.WriteBuffer;
import org.apache.activemq.artemis.core.journal.EncodingSupport;
import org.apache.activemq.artemis.concurrent.ringbuffer.BytesUtils;

public final class BatchWriteBuffer implements WriteBuffer {

   private final Thread writerProcessor;
   private final AtomicBoolean running;
   private final BatchWriteProcessor batchWriteProcessor;
   private final BatchWriteCommandExecutorAgent commandExecutorAgent;
   private final int messageCapacity;

   public BatchWriteBuffer(final int maxBatchSize, long checkPointNanos) {
      final RingBuffers.RingBufferType ringBufferType = RingBuffers.RingBufferType.SingleProducerSingleConsumer;
      //calculates how many messages of averageMessageSize length are necessary to contains maxBatchSize
      final int averageMessageSize = BytesUtils.CACHE_LINE_LENGTH * 8;
      final int messageCount = (int) (BytesUtils.align(maxBatchSize, averageMessageSize) / averageMessageSize);
      final int requiredCapacity = RingBuffers.capacity(ringBufferType, messageCount, averageMessageSize);
      final RefRingBuffer<WriteRequest> writeRequests;
      final ByteBuffer bytes = ByteBuffer.allocateDirect(requiredCapacity);
      writeRequests = RingBuffers.withRef(ringBufferType, bytes, WriteRequest::new, averageMessageSize);
      this.messageCapacity = writeRequests.refCapacity();
      this.running = new AtomicBoolean(false);
      this.writerProcessor = new Thread(this::dutyCycleLoop);
      this.batchWriteProcessor = new BatchWriteProcessorProxy(writeRequests, maxBatchSize, running);
      this.commandExecutorAgent = new BatchWriteCommandExecutorAgent(writeRequests, new BatchAppender(checkPointNanos));
   }

   public int messageCapacity() {
      return this.messageCapacity;
   }

   private void dutyCycleLoop() {
      int waiTimes = 0;
      final BatchWriteCommandExecutorAgent commandExecutorAgent = this.commandExecutorAgent;
      while (!commandExecutorAgent.isClosed()) {
         if (commandExecutorAgent.doWork() > 0) {
            waiTimes = 0;
         } else {
            if (waiTimes < 10) {
               waiTimes++;
            } else if (waiTimes < 20) {
               Thread.yield();
               waiTimes++;
            } else if (waiTimes < 50) {
               LockSupport.parkNanos(1L);
               waiTimes++;
            } else if (waiTimes < 100) {
               LockSupport.parkNanos(100L);
               waiTimes++;
            } else {
               LockSupport.parkNanos(1000L);
            }
         }
      }
      commandExecutorAgent.cleanUp();
   }

   private void checkRecursion() {
      if (Thread.currentThread() == writerProcessor) {
         throw new IllegalStateException("can't request this operation from the writer thread!");
      }
   }

   private void checkIsRunning() {
      if (!running.get()) {
         throw new IllegalStateException("WriteBuffer is not started");
      }
   }

   @Override
   public void start() {
      checkRecursion();
      if (running.compareAndSet(false, true)) {
         writerProcessor.start();
      }
   }

   @Override
   public void stop() {
      checkRecursion();
      if (running.compareAndSet(true, false)) {
         this.batchWriteProcessor.close();
         try {
            writerProcessor.join();
         } catch (InterruptedException e) {
            throw new IllegalStateException(e);
         }
      }
   }

   @Override
   public void setObserver(final TimedBufferObserver observer) {
      checkRecursion();
      checkIsRunning();
      this.batchWriteProcessor.setObserver((SequentialFileWriter) observer);
   }

   /**
    * Verify if the size fits the buffer
    *
    * @param sizeChecked
    */
   @Override
   public boolean checkSize(final int sizeChecked) {
      return this.batchWriteProcessor.checkSize(sizeChecked);
   }

   @Override
   public void addBytes(final ActiveMQBuffer bytes, final boolean sync, final IOCallback callback) {
      checkRecursion();
      checkIsRunning();
      this.batchWriteProcessor.addBytes(bytes, sync, callback);
   }

   @Override
   public void addBytes(final EncodingSupport bytes, final boolean sync, final IOCallback callback) {
      checkRecursion();
      checkIsRunning();
      this.batchWriteProcessor.addBytes(bytes, sync, callback);
   }

   @Override
   public void flush() {
      checkRecursion();
      checkIsRunning();
      this.batchWriteProcessor.flush();
   }

}
