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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.concurrent.ThreadFactory;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.apache.activemq.artemis.jlibaio.LibaioContext;
import org.apache.activemq.artemis.jlibaio.LibaioFile;
import org.apache.activemq.artemis.jlibaio.SubmitInfo;

public final class RotatingFileAppender implements AutoCloseable {

   private static final class IOCallbackMatcher implements EventHandler<FileOperation> {

      private final BitSet callbacksCompleted;
      private final LibaioContext<FileOperation> context;
      private final FileOperation[] callbacks;
      private final int maxIO;

      IOCallbackMatcher(LibaioContext<FileOperation> context, int maxIO) {
         this.context = context;
         this.callbacks = new FileOperation[maxIO];
         this.callbacksCompleted = new BitSet(maxIO);
         this.maxIO = maxIO;
      }

      @Override
      public void onEvent(FileOperation fileOperation, long seq, boolean endOfBatch) throws Exception {
         final SubmitInfo ioCallback = fileOperation.ioCallback;
         final int id = fileOperation.id;
         try {
            while (!callbacksCompleted.get(id)) {
               final int completed = context.poll(callbacks, 1, maxIO);
               for (int i = 0; i < completed; i++) {
                  final FileOperation completedFileOperation = callbacks[i];
                  callbacksCompleted.set(completedFileOperation.id);
                  callbacks[i] = null;
               }
            }
            callbacksCompleted.clear(id);
            if (fileOperation.fileToClose != null) {
               fileOperation.fileToClose.close();
            }
            //ordered (== submit order) sequence of completion: this could be off-loaded to a separate thread too
            if (fileOperation.errorMessage != null) {
               ioCallback.onError(fileOperation.errno, fileOperation.errorMessage);
            } else {
               ioCallback.done();
            }
         } finally {
            fileOperation.reset();
         }
      }
   }

   private static final class FileOperationSubmitter implements EventHandler<FileOperation> {

      private final LibaioContext<FileOperation> context;
      private int journalFileId;
      private final File journalDir;
      private final long fileSize;
      private final boolean fillBeforeWrite;
      private LibaioFile<FileOperation> file = null;
      private int fileWriteIndex = 0;
      private final int alignment;

      FileOperationSubmitter(File journalDir,
                             LibaioContext<FileOperation> context,
                             int fileSize,
                             int alignment,
                             boolean fillBeforeWrite) {
         this.journalFileId = 0;
         this.fileSize = fileSize;
         this.context = context;
         this.journalDir = journalDir;
         this.fillBeforeWrite = fillBeforeWrite;
         this.alignment = alignment;
      }

      @Override
      public void onEvent(FileOperation fileOperation, long seq, boolean endOfBatch) throws IOException {
         final ByteBuffer writeBuffer = fileOperation.writeBuffer;
         writeBuffer.flip();
         final int requiredBytes = align(writeBuffer.remaining(), alignment);
         LibaioFile<FileOperation> currentFile = file;
         if (file == null || (fileWriteIndex + requiredBytes) > fileSize) {
            //send the old file to be closed (if any) when this operation will complete
            fileOperation.fileToClose = file;
            file = context.openFile(new File(journalDir, Integer.toString(this.journalFileId)), true);
            fileWriteIndex = 0;
            if (fillBeforeWrite) {
               file.fill(alignment, fileSize);
            }
            this.journalFileId++;
            currentFile = file;
         } else if (fileWriteIndex + requiredBytes == fileSize) {
            //try to close the operation ASAP
            fileOperation.fileToClose = file;
            currentFile = file;
            file = null;
         }
         currentFile.write(fileWriteIndex, requiredBytes, fileOperation.writeBuffer, fileOperation);
         fileWriteIndex += requiredBytes;
      }
   }

   private static final class FileOperation implements SubmitInfo {

      private static int ID_GENERATOR = 0;

      private final int id = ID_GENERATOR++;
      private ByteBuffer writeBuffer = null;
      //TODO it will be a IOCallback in a real version
      private SubmitInfo ioCallback;
      private LibaioFile<FileOperation> fileToClose;
      private boolean fsync;
      private int errno;
      private String errorMessage;

      @Override
      public void onError(int errno, String message) {
         this.errno = errno;
         this.errorMessage = message;
      }

      @Override
      public void done() {
         throw new IllegalStateException("the blockPoll shouldn't call directly done");
      }

      public void reset() {
         errorMessage = null;
         if (writeBuffer != null) {
            writeBuffer.clear();
         }
         ioCallback = null;
         fileToClose = null;
      }
   }

   private final Disruptor<FileOperation> disruptor;
   private final LibaioContext<FileOperation> context;
   private RingBuffer<FileOperation> operations;
   private final int alignment;

   public RotatingFileAppender(final File journalDir,
                               final int fileSize,
                               final int maxIO,
                               final ThreadFactory threadFactory,
                               final ProducerType producerType,
                               final WaitStrategy waitStrategy,
                               final boolean fillBeforeWrite,
                               final boolean dataSync) {
      context = new LibaioContext(maxIO, false, dataSync);
      alignment = calculateAlignment(journalDir);
      disruptor = new Disruptor<>(FileOperation::new, maxIO, threadFactory, producerType, waitStrategy);
      disruptor.setDefaultExceptionHandler(new ExceptionHandler<FileOperation>() {
         @Override
         public void handleEventException(Throwable ex, long sequence, FileOperation event) {
            ex.printStackTrace();
         }

         @Override
         public void handleOnStartException(Throwable ex) {
            ex.printStackTrace();
         }

         @Override
         public void handleOnShutdownException(Throwable ex) {
            ex.printStackTrace();
         }
      });
      disruptor.handleEventsWith(new FileOperationSubmitter(journalDir, context, align(fileSize, alignment), alignment, fillBeforeWrite)).then(new IOCallbackMatcher(context, maxIO));
      operations = null;
   }

   private static int calculateAlignment(final File journalDir) {
      try {
         final File checkFile = new File(journalDir, "journalCheck.tmp");
         checkFile.deleteOnExit();
         checkFile.mkdirs();
         checkFile.createNewFile();
         final int alignment = LibaioContext.getBlockSize(checkFile);
         checkFile.delete();
         return alignment;
      } catch (IOException e) {
         //TODO print me please
         return 512;
      }
   }

   public void start() {
      this.operations = disruptor.start();
   }

   private static int align(final int value, final int alignment) {
      return (value + (alignment - 1)) & ~(alignment - 1);
   }

   public void append(ByteBuffer bytes, boolean fsync, SubmitInfo ioCallback) {
      if (bytes.position() == 0) {
         return;
      }
      final long sequence = operations.next();
      try {
         bytes.flip();
         final int requiredBytes = bytes.limit();
         final FileOperation fileOperation = operations.get(sequence);
         ByteBuffer writeBuffer = fileOperation.writeBuffer;
         if (writeBuffer == null || writeBuffer.remaining() < requiredBytes) {
            if (writeBuffer != null) {
               LibaioContext.freeBuffer(writeBuffer);
            }
            writeBuffer = LibaioContext.newAlignedBuffer(align(requiredBytes, alignment), alignment);
            fileOperation.writeBuffer = writeBuffer;
         }
         writeBuffer.put(bytes);
         fileOperation.ioCallback = ioCallback;
         fileOperation.fsync = fsync;
      } finally {
         operations.publish(sequence);
      }
   }

   @Override
   public void close() {
      try {
         disruptor.shutdown();
      } finally {
         operations = null;

      }
   }

}
