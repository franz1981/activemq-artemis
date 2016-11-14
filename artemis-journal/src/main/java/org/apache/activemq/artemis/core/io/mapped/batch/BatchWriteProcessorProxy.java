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

package org.apache.activemq.artemis.core.io.mapped.batch;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.locks.LockSupport;

import io.netty.buffer.Unpooled;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.concurrent.ringbuffer.MessageRefTranslator;
import org.apache.activemq.artemis.concurrent.ringbuffer.RefRingBuffer;
import org.apache.activemq.artemis.core.buffers.impl.ChannelBufferWrapper;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.journal.EncodingSupport;

final class BatchWriteProcessorProxy implements BatchWriteProcessor {

   private final RefRingBuffer<WriteRequest> writeRequests;
   private final ThreadLocal<ActiveMQBuffer> writeRequestsWrapperPool;
   private final ThreadLocal<ReusableIOCallback> callbackPool;
   private final AtomicBoolean isRunning;
   private final int maxBatchSize;
   private final MessageRefTranslator<WriteRequest, ActiveMQBuffer, IOCallback> writeActiveMQBuffer;
   private final MessageRefTranslator<WriteRequest, EncodingSupport, IOCallback> writeEncodingSupport;
   private int remainingBytes;

   BatchWriteProcessorProxy(final RefRingBuffer<WriteRequest> writeRequests,
                            int maxBatchSize,
                            AtomicBoolean isRunning) {
      this.writeRequests = writeRequests;
      this.remainingBytes = 0;
      this.maxBatchSize = maxBatchSize;
      this.writeRequestsWrapperPool = ThreadLocal.withInitial(() -> new ChannelBufferWrapper(Unpooled.wrappedBuffer(writeRequests.buffer())));
      this.callbackPool = ThreadLocal.withInitial(ReusableIOCallback::new);
      this.isRunning = isRunning;
      this.writeActiveMQBuffer = this::write;
      this.writeEncodingSupport = this::write;
   }

   @Override
   public void setObserver(SequentialFileWriter observer) {
      final ReusableIOCallback ioCallback = callbackPool.get().reset();
      try {
         while (this.writeRequests.write(BatchWriteCommands.CHANGE_OBSERVER_REQUEST_MSG_ID, 0, (writeRequest, msgType, bytes, offset, length, timedBufferObserver, callback) -> {
            writeRequest.observer = timedBufferObserver;
            writeRequest.callback = callback;
         }, observer, ioCallback) < 0) {
            checkIsRunning();
         }
         //TODO add a wait timeout!!!!
         while (!ioCallback.isDone()) {
            LockSupport.parkNanos(1L);
         }
         final String errorMessage = ioCallback.errorMessage();
         if (errorMessage != null) {
            throw new IllegalStateException(errorMessage);
         }
         //WARNING: any write must stop on this call and only one thread at time can perform it
         if (observer != null) {
            //getRemainingBytes is not thread safe but any writes before the callback will be published
            //after the callback!
            final int remainingBytes = observer.getRemainingBytes();
            assert remainingBytes >= 0;
            this.remainingBytes = remainingBytes;
         } else {
            this.remainingBytes = 0;
         }
      } finally {
         ioCallback.reset();
      }
   }

   @Override
   public boolean checkSize(int sizeChecked) {
      if (sizeChecked > writeRequests.maxMessageLength()) {
         throw new IllegalStateException("Can't write records bigger than the bufferSize(" + writeRequests.maxMessageLength() + ") on the journal");
      }
      return (this.remainingBytes >= sizeChecked);
   }

   private void checkMessageLength(int requestedLength) {
      if (requestedLength > this.maxBatchSize) {
         throw new IllegalArgumentException("requested length can't be more than maxBatchSize!");
      }
   }

   private void write(WriteRequest writeRequest,
                      int msgType,
                      ByteBuffer byteBuffer,
                      int offset,
                      int length,
                      ActiveMQBuffer activeMQBuffer,
                      IOCallback callback) {
      final ActiveMQBuffer writeBuffer = this.writeRequestsWrapperPool.get();
      try {
         writeBuffer.clear();
         writeBuffer.writerIndex(offset);
         writeBuffer.writeBytes(activeMQBuffer, activeMQBuffer.readerIndex(), length);
         writeRequest.callback = callback;
      } finally {
         writeBuffer.clear();
      }
   }

   private void write(WriteRequest writeRequest,
                      int msgType,
                      ByteBuffer byteBuffer,
                      int offset,
                      int length,
                      EncodingSupport encodingSupport,
                      IOCallback callback) {
      final ActiveMQBuffer writeBuffer = this.writeRequestsWrapperPool.get();
      try {
         writeBuffer.clear();
         writeBuffer.writerIndex(offset);
         encodingSupport.encode(writeBuffer);
         writeRequest.callback = callback;
      } finally {
         writeBuffer.clear();
      }
   }

   @Override
   public void addBytes(final ActiveMQBuffer activeMQBuffer, final boolean sync, final IOCallback callback) {
      final int encodedSize = activeMQBuffer.readableBytes();
      checkMessageLength(encodedSize);
      if (this.remainingBytes < encodedSize) {
         throw new IllegalStateException("insufficient space to addBytes!");
      }
      final int msgId = sync ? BatchWriteCommands.WRITE_AND_FLUSH_REQUEST_MSG_ID : BatchWriteCommands.WRITE_REQUEST_MSG_ID;
      while (this.writeRequests.write(msgId, encodedSize, this.writeActiveMQBuffer, activeMQBuffer, callback) < 0) {
         checkIsRunning();
      }
      this.remainingBytes -= encodedSize;
      assert remainingBytes >= 0;
   }

   @Override
   public void addBytes(final EncodingSupport encodingSupport, final boolean sync, final IOCallback callback) {
      final int encodedSize = encodingSupport.getEncodeSize();
      checkMessageLength(encodedSize);
      if (this.remainingBytes < encodedSize) {
         throw new IllegalStateException("insufficient space to addBytes!");
      }
      final int msgId = sync ? BatchWriteCommands.WRITE_AND_FLUSH_REQUEST_MSG_ID : BatchWriteCommands.WRITE_REQUEST_MSG_ID;
      while (this.writeRequests.write(msgId, encodedSize, this.writeEncodingSupport, encodingSupport, callback) < 0) {
         checkIsRunning();
      }
      this.remainingBytes -= encodedSize;
      assert remainingBytes >= 0;
   }

   @Override
   public void flush() {
      final ReusableIOCallback ioCallback = callbackPool.get().reset();
      try {
         while (this.writeRequests.write(BatchWriteCommands.FLUSH_REQUEST_MSG_ID, 0, (writeRequest, msgType, bytes, offset, length, callback, nil) -> writeRequest.callback = callback, ioCallback, null) < 0) {
            checkIsRunning();
         }
         while (!ioCallback.isDone()) {
            //need a timeout while "waiting"
            LockSupport.parkNanos(1L);
         }
         final String errorMessage = ioCallback.errorMessage();
         if (errorMessage != null) {
            throw new IllegalStateException(errorMessage);
         }
      } finally {
         ioCallback.reset();
      }
   }

   @Override
   public void close() {
      while (writeRequests.write(BatchWriteCommands.CLOSE_REQUEST_MSG_ID, 0, (msgType, bytes, offset, length, nil) -> {
      }, null) < 0) {
         checkIsRunning();
      }
   }

   private void checkIsRunning() {
      if (!isRunning.get()) {
         throw new IllegalStateException("WriteBuffer is not running!");
      }
   }

   //SINGLE WRITER CALLBACK
   private static final class ReusableIOCallback implements IOCallback {

      private static final int WAIT = 0;
      private static final int DONE = 1;

      private static final AtomicIntegerFieldUpdater<ReusableIOCallback> STATE_UPDATER = AtomicIntegerFieldUpdater.newUpdater(ReusableIOCallback.class, "state");

      private volatile int state = WAIT;
      private int errorCode = 0;
      private String errorMessage = null;

      //is safe to call it from every thread!
      public boolean isDone() {
         //ACQUIRE or LoadLoad + LoadStore
         return STATE_UPDATER.get(this) == DONE;
      }

      //is safe to call it after isDone == true
      public int errorCode() {
         return errorCode;
      }

      //is safe to call it after isDone == true
      public String errorMessage() {
         return errorMessage;
      }

      //single writer
      @Override
      public void done() {
         this.errorCode = 0;
         this.errorMessage = null;
         //WriteWrite
         STATE_UPDATER.lazySet(this, DONE);
      }

      //single writer
      @Override
      public void onError(int errorCode, String errorMessage) {
         this.errorCode = errorCode;
         this.errorMessage = errorMessage;
         //WriteWrite
         STATE_UPDATER.lazySet(this, DONE);
      }

      //single writer
      private ReusableIOCallback reset() {
         this.errorCode = 0;
         this.errorMessage = null;
         STATE_UPDATER.lazySet(this, WAIT);
         return this;
      }
   }
}
