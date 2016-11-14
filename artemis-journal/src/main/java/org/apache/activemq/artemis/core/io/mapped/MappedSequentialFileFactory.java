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
package org.apache.activemq.artemis.core.io.mapped;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import io.netty.util.internal.PlatformDependent;
import org.apache.activemq.artemis.core.io.IOCriticalErrorListener;
import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.core.io.SequentialFileFactory;
import org.apache.activemq.artemis.core.io.mapped.batch.BatchWriteBuffer;
import org.apache.activemq.artemis.concurrent.ringbuffer.BytesUtils;

public final class MappedSequentialFileFactory implements SequentialFileFactory {

   private final File directory;
   private final IOCriticalErrorListener criticalErrorListener;
   private final BatchWriteBuffer timedBuffer;
   private final int maxIO;
   private final int capacity;
   private boolean useDataSync;
   private boolean supportCallbacks;

   public MappedSequentialFileFactory(File directory,
                                      int capacity,
                                      IOCriticalErrorListener criticalErrorListener,
                                      boolean supportCallbacks,
                                      BatchWriteBuffer writeBuffer) {
      this.directory = directory;
      this.capacity = capacity;
      this.criticalErrorListener = criticalErrorListener;
      this.useDataSync = true;
      this.timedBuffer = writeBuffer;
      this.supportCallbacks = supportCallbacks;
      this.maxIO = writeBuffer == null ? 1 : writeBuffer.messageCapacity();
      if (capacity <= 0) {
         throw new IllegalStateException("capacity must be > 0!");
      }
   }

   public MappedSequentialFileFactory(File directory,
                                      int capacity,
                                      IOCriticalErrorListener criticalErrorListener,
                                      boolean supportCallbacks) {
      this(directory, capacity, criticalErrorListener, supportCallbacks, null);
   }

   public MappedSequentialFileFactory(File directory, int capacity, IOCriticalErrorListener criticalErrorListener) {
      this(directory, capacity, criticalErrorListener, false);
   }

   public MappedSequentialFileFactory(File directory, int capacity) {
      this(directory, capacity, null);
   }

   @Override
   public SequentialFile createSequentialFile(String fileName) {
      final MappedSequentialFile mappedSequentialFile = new MappedSequentialFile(this, directory, new File(directory, fileName), capacity, criticalErrorListener);
      if (this.timedBuffer == null) {
         return mappedSequentialFile;
      } else {
         return new TimedSequentialFile(this, mappedSequentialFile);
      }
   }

   @Override
   public SequentialFileFactory setDatasync(boolean enabled) {
      this.useDataSync = enabled;
      return this;
   }

   @Override
   public boolean isDatasync() {
      return useDataSync;
   }

   @Override
   public int getMaxIO() {
      return this.maxIO;
   }

   @Override
   public List<String> listFiles(final String extension) throws Exception {
      final FilenameFilter extensionFilter = (file, name) -> name.endsWith("." + extension);
      final String[] fileNames = directory.list(extensionFilter);
      if (fileNames == null) {
         return Collections.EMPTY_LIST;
      }
      return Arrays.asList(fileNames);
   }

   @Override
   public boolean isSupportsCallbacks() {
      return this.supportCallbacks;
   }

   @Override
   public void onIOError(Exception exception, String message, SequentialFile file) {
      if (criticalErrorListener != null) {
         criticalErrorListener.onIOException(exception, message, file);
      }
   }

   @Override
   public ByteBuffer allocateDirectBuffer(final int size) {
      return ByteBuffer.allocateDirect(size);
   }

   @Override
   public void releaseDirectBuffer(final ByteBuffer buffer) {
      PlatformDependent.freeDirectBuffer(buffer);
   }

   @Override
   public ByteBuffer newBuffer(final int size) {
      return ByteBuffer.allocate(size);
   }

   @Override
   public void releaseBuffer(ByteBuffer buffer) {
      if (buffer.isDirect()) {
         PlatformDependent.freeDirectBuffer(buffer);
      }
   }

   @Override
   public void activateBuffer(SequentialFile file) {
      if (timedBuffer != null) {
         file.setTimedBuffer(timedBuffer);
      }
   }

   @Override
   public void deactivateBuffer() {
      if (timedBuffer != null) {
         // Removing the observer force the timed buffer to flush any pending changes
         timedBuffer.setObserver(null);
      }
   }

   @Override
   public ByteBuffer wrapBuffer(final byte[] bytes) {
      return ByteBuffer.wrap(bytes);
   }

   @Override
   public int getAlignment() {
      return 1;
   }

   @Override
   @Deprecated
   public MappedSequentialFileFactory setAlignment(int alignment) {
      throw new UnsupportedOperationException();
   }

   @Override
   public int calculateBlockSize(int bytes) {
      return bytes;
   }

   @Override
   public File getDirectory() {
      return this.directory;
   }

   @Override
   public void clearBuffer(final ByteBuffer buffer) {
      if (buffer.isDirect()) {
         BytesUtils.zeros(buffer);
      } else if (buffer.hasArray()) {
         final byte[] array = buffer.array();
         //SIMD OPTIMIZATION
         Arrays.fill(array, (byte) 0);
      } else {
         //TODO VERIFY IF IT COULD HAPPENS
         final int capacity = buffer.capacity();
         for (int i = 0; i < capacity; i++) {
            buffer.put(i, (byte) 0);
         }
      }
      buffer.rewind();
   }

   @Override
   public void start() {
      if (timedBuffer != null) {
         timedBuffer.start();
      }
   }

   @Override
   public void stop() {
      if (timedBuffer != null) {
         timedBuffer.stop();
      }
   }

   @Override
   public void createDirs() throws Exception {
      boolean ok = directory.mkdirs();
      if (!ok) {
         throw new IOException("Failed to create directory " + directory);
      }
   }

   @Override
   public void flush() {
      if (timedBuffer != null) {
         timedBuffer.flush();
      }
   }
}
