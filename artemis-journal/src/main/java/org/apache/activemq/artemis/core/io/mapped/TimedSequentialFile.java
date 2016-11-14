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

package org.apache.activemq.artemis.core.io.mapped;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQExceptionType;
import org.apache.activemq.artemis.core.io.DummyCallback;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.core.io.buffer.TimedBufferObserver;
import org.apache.activemq.artemis.core.io.buffer.WriteBuffer;
import org.apache.activemq.artemis.core.io.mapped.batch.SequentialFileWriter;
import org.apache.activemq.artemis.core.journal.EncodingSupport;
import org.apache.activemq.artemis.journal.ActiveMQJournalLogger;

final class TimedSequentialFile implements SequentialFile {

   //no pretoucher is enabled by default!
   private static final MappedPretoucher PRETOUCHER = MappedPretoucher.noOp();
   private final MappedSequentialFileFactory factory;
   private final MappedSequentialFile sequentialFile;
   private final TimedBufferObserver observer;
   private final ThreadLocal<ResettableIOCallback> callbackPool;
   private WriteBuffer timedBuffer;

   TimedSequentialFile(MappedSequentialFileFactory factory, MappedSequentialFile sequentialFile) {
      this.sequentialFile = sequentialFile;
      this.factory = factory;
      this.observer = new MappedFileWriter();
      this.callbackPool = ThreadLocal.withInitial(ResettableIOCallback::new);
   }

   @Override
   public boolean isOpen() {
      return this.sequentialFile.isOpen();
   }

   @Override
   public boolean exists() {
      return this.sequentialFile.exists();
   }

   @Override
   public void open() throws Exception {
      this.sequentialFile.open();
      this.timedBuffer = null;
   }

   @Override
   public void open(int maxIO, boolean useExecutor) throws Exception {
      this.sequentialFile.open(maxIO, useExecutor);
      this.timedBuffer = null;
   }

   @Override
   public boolean fits(int size) {
      if (timedBuffer == null) {
         return this.sequentialFile.fits(size);
      } else {
         return timedBuffer.checkSize(size);
      }
   }

   @Override
   public int calculateBlockStart(int position) throws Exception {
      return this.sequentialFile.calculateBlockStart(position);
   }

   @Override
   public String getFileName() {
      return this.sequentialFile.getFileName();
   }

   @Override
   public void fill(int size) throws Exception {
      if (timedBuffer != null) {
         throw new IllegalStateException("unsupported fill when there is an active write buffer!");
      }
      this.sequentialFile.fill(size);
   }

   @Override
   public void delete() throws IOException, InterruptedException, ActiveMQException {
      this.sequentialFile.delete();
   }

   @Override
   public void write(ActiveMQBuffer bytes, boolean sync, IOCallback callback) throws Exception {
      if (this.timedBuffer == null) {
         throw new IllegalStateException("must set a proper write buffer first!");
      }
      this.timedBuffer.addBytes(bytes, sync, callback);
   }

   @Override
   public void write(ActiveMQBuffer bytes, boolean sync) throws Exception {
      if (this.timedBuffer == null) {
         throw new IllegalStateException("must set a proper write buffer first!");
      }
      if (sync) {
         final ResettableIOCallback callback = callbackPool.get();
         try {
            this.timedBuffer.addBytes(bytes, true, callback);
            callback.waitCompletion();
         } finally {
            callback.reset();
         }
      } else {
         this.timedBuffer.addBytes(bytes, false, DummyCallback.getInstance());
      }
   }

   @Override
   public void write(EncodingSupport bytes, boolean sync, IOCallback callback) throws Exception {
      if (timedBuffer == null) {
         throw new IllegalStateException("must set a proper write buffer first!");
      }
      this.timedBuffer.addBytes(bytes, sync, callback);
   }

   @Override
   public void write(EncodingSupport bytes, boolean sync) throws Exception {
      if (timedBuffer == null) {
         throw new IllegalStateException("must set a proper write buffer first!");
      }
      if (sync) {
         final ResettableIOCallback callback = callbackPool.get();
         try {
            this.timedBuffer.addBytes(bytes, true, callback);
            callback.waitCompletion();
         } finally {
            callback.reset();
         }
      } else {
         this.timedBuffer.addBytes(bytes, false, DummyCallback.getInstance());
      }
   }

   @Override
   public void writeDirect(ByteBuffer bytes, boolean sync, IOCallback callback) {
      if (timedBuffer != null) {
         throw new IllegalStateException("unsupported direct writes when a write buffer is specified!");
      }
      this.sequentialFile.writeDirect(bytes, sync, callback);
   }

   @Override
   public void writeDirect(ByteBuffer bytes, boolean sync) throws Exception {
      if (timedBuffer != null) {
         throw new IllegalStateException("unsupported direct writes when a write buffer is specified!");
      }
      this.sequentialFile.writeDirect(bytes, sync);
   }

   @Override
   public int read(ByteBuffer bytes, IOCallback callback) throws Exception {
      if (timedBuffer != null) {
         throw new IllegalStateException("unsupported reads when a write buffer is specified!");
      }
      return this.sequentialFile.read(bytes, callback);
   }

   @Override
   public int read(ByteBuffer bytes) throws Exception {
      if (timedBuffer != null) {
         throw new IllegalStateException("unsupported reads when a write buffer is specified!");
      }
      return this.sequentialFile.read(bytes);
   }

   @Override
   public void position(long pos) throws IOException {
      if (timedBuffer != null) {
         throw new IllegalStateException("can't change position when there is an activated write buffer!");
      }
      this.sequentialFile.position(pos);
   }

   @Override
   public long position() {
      return this.sequentialFile.position();
   }

   @Override
   public void close() throws Exception {
      this.sequentialFile.close();
      this.timedBuffer = null;
   }

   @Override
   public void sync() throws IOException {
      if (this.timedBuffer == null) {
         this.sequentialFile.sync();
      } else {
         this.timedBuffer.flush();
      }
   }

   @Override
   public long size() throws Exception {
      return this.sequentialFile.size();
   }

   @Override
   public void renameTo(String newFileName) throws Exception {
      this.sequentialFile.renameTo(newFileName);
   }

   @Override
   public SequentialFile cloneFile() {
      return new TimedSequentialFile(factory, this.sequentialFile.cloneFile());
   }

   @Override
   public void copyTo(SequentialFile newFileName) throws Exception {
      this.sequentialFile.copyTo(newFileName);
   }

   @Override
   public void setTimedBuffer(WriteBuffer buffer) {
      if (buffer == null) {
         throw new IllegalArgumentException("can't set a null write buffer!");
      }
      this.timedBuffer = buffer;
      buffer.setObserver(this.observer);
   }

   @Override
   public File getJavaFile() {
      return this.sequentialFile.getJavaFile();
   }

   private static final class ResettableIOCallback implements IOCallback {

      private final CyclicBarrier cyclicBarrier;
      private int errorCode;
      private String errorMessage;

      ResettableIOCallback() {
         this.cyclicBarrier = new CyclicBarrier(2);
      }

      public void waitCompletion() throws InterruptedException, ActiveMQException, BrokenBarrierException {
         this.cyclicBarrier.await();
         if (this.errorMessage != null) {
            throw ActiveMQExceptionType.createException(this.errorCode, this.errorMessage);
         }
      }

      public void reset() {
         this.errorCode = 0;
         this.errorMessage = null;
      }

      @Override
      public void done() {
         try {
            this.cyclicBarrier.await();
         } catch (BrokenBarrierException | InterruptedException e) {
            throw new IllegalStateException(e);
         }
      }

      @Override
      public void onError(int errorCode, String errorMessage) {
         try {
            this.errorCode = errorCode;
            this.errorMessage = errorMessage;
            this.cyclicBarrier.await();
         } catch (BrokenBarrierException | InterruptedException e) {
            throw new IllegalStateException(e);
         }
      }
   }

   private static final class DelegateCallback implements IOCallback {

      private List<IOCallback> callbacks;

      private DelegateCallback() {
         callbacks = null;
      }

      public DelegateCallback delegates(List<IOCallback> callbacks) {
         this.callbacks = callbacks;
         return this;
      }

      @Override
      public void done() {
         if (callbacks != null) {
            final int size = callbacks.size();
            for (int i = 0; i < size; i++) {
               try {
                  final IOCallback callback = callbacks.get(i);
                  callback.done();
               } catch (Throwable e) {
                  ActiveMQJournalLogger.LOGGER.errorCompletingCallback(e);
               }
            }
         }
      }

      @Override
      public void onError(final int errorCode, final String errorMessage) {
         if (callbacks != null) {
            final int size = callbacks.size();
            for (int i = 0; i < size; i++) {
               try {
                  final IOCallback callback = callbacks.get(i);
                  callback.onError(errorCode, errorMessage);
               } catch (Throwable e) {
                  ActiveMQJournalLogger.LOGGER.errorCallingErrorCallback(e);
               }
            }
         }
      }
   }

   final class MappedFileWriter implements SequentialFileWriter {

      private final DelegateCallback delegateCallback = new DelegateCallback();

      @Override
      public void flushBuffer(ByteBuffer buffer,
                              int index,
                              int length,
                              boolean requestedSync,
                              List<IOCallback> callbacks) {
         if (length == 0) {
            if (requestedSync) {
               //simple sync request necessary in case of writes done outside the batch buffer, but requested to be finalized
               //into with the batch buffer!
               try {
                  sequentialFile.sync();
                  //everything ok!!
                  final int size = callbacks.size();
                  for (int i = 0; i < size; i++) {
                     callbacks.get(i).done();
                  }
               } catch (Throwable e) {
                  //errors :(
                  final int size = callbacks.size();
                  for (int i = 0; i < size; i++) {
                     callbacks.get(i).onError(ActiveMQExceptionType.IO_ERROR.getCode(), e.getLocalizedMessage());
                  }
               }
            } else {
               //release the callbacks
               final int size = callbacks.size();
               for (int i = 0; i < size; i++) {
                  callbacks.get(i).done();
               }
            }
         } else {
            this.delegateCallback.delegates(callbacks);
            try {
               PRETOUCHER.pretouch(sequentialFile.mappedFile(), sequentialFile.mappedFile().position() + length);
               sequentialFile.writeDirect(buffer, index, length, requestedSync, this.delegateCallback);
            } finally {
               this.delegateCallback.delegates(null);
            }
         }
      }

      @Override
      public void flushBuffer(final ByteBuffer buffer, final boolean requestedSync, final List<IOCallback> callbacks) {
         buffer.flip();
         if (buffer.limit() == 0) {
            if (requestedSync) {
               //simple sync request necessary in case of writes done outside the batch buffer, but requested to be finalized
               //into with the batch buffer!
               try {
                  sequentialFile.sync();
                  //everything ok!!
                  final int size = callbacks.size();
                  for (int i = 0; i < size; i++) {
                     callbacks.get(i).done();
                  }
               } catch (Throwable e) {
                  //errors :(
                  final int size = callbacks.size();
                  for (int i = 0; i < size; i++) {
                     callbacks.get(i).onError(ActiveMQExceptionType.IO_ERROR.getCode(), e.getLocalizedMessage());
                  }
               }
            } else {
               //release the callbacks
               final int size = callbacks.size();
               for (int i = 0; i < size; i++) {
                  callbacks.get(i).done();
               }
            }
         } else {
            this.delegateCallback.delegates(callbacks);
            try {
               PRETOUCHER.pretouch(sequentialFile.mappedFile(), sequentialFile.mappedFile().position() + buffer.limit());
               sequentialFile.writeDirect(buffer, requestedSync, delegateCallback);
            } finally {
               this.delegateCallback.delegates(null);
            }
         }
      }

      @Override
      public ByteBuffer newBuffer(final int size, final int limit) {
         final int alignedSize = factory.calculateBlockSize(size);
         final int alignedLimit = factory.calculateBlockSize(limit);
         final ByteBuffer buffer = factory.newBuffer(alignedSize);
         buffer.limit(alignedLimit);
         return buffer;
      }

      @Override
      public int getRemainingBytes() {
         try {
            final int remaining;
            if (sequentialFile.isOpen()) {
               remaining = (int) Math.min(sequentialFile.size() - sequentialFile.position(), Integer.MAX_VALUE);
            } else {
               return (int) Math.min(sequentialFile.size(), Integer.MAX_VALUE);
            }
            return remaining;
         } catch (Exception e) {
            throw new IllegalStateException(e);
         }
      }

      @Override
      public void onDeactivatedTimedBuffer() {
         assert TimedSequentialFile.this.timedBuffer != null;
         TimedSequentialFile.this.timedBuffer = null;
      }

      @Override
      public String toString() {
         return "TimedBufferObserver on file (" + getFileName() + ")";
      }

   }
}
