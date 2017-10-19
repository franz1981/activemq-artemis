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

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import org.apache.activemq.artemis.api.core.ActiveMQIOErrorException;

/**
 * It allows zero-copy operations (if the OS/kernel will support it).
 */
public interface TransferableSequentialFile extends SequentialFile {

   /**
    * The use of the {@link FileChannel} is not encouraged: it is mainly exposed for internal uses only (eg {@link #transferTo(long, long, WritableByteChannel)} ).
    */
   FileChannel channel();

   /**
    * {@see FileChannel#transferTo}.
    * Do not allow destination to grow beyond {@link #capacity()} and do not change {@link #position()} of {@code this} file.
    */
   default long transferTo(long position, long count, TransferableSequentialFile destination) throws IOException {
      if (!this.isOpen() || !destination.isOpen()) {
         throw new IllegalStateException("both files must be opened!");
      }
      //check available space on source
      final long effectiveSrcCount = effectiveCount(position, count, size());
      if (effectiveSrcCount <= 0) {
         return 0;
      }
      //check available space on destination === do not allow a file to grow over capacity
      final long dstPosition = destination.position();
      final long effectiveDstCount = effectiveCount(dstPosition, effectiveSrcCount, destination.capacity());
      if (effectiveDstCount <= 0) {
         return 0;
      }
      //move destination
      final FileChannel dstChannel = destination.channel();
      dstChannel.position(dstPosition);
      final long bytes = channel().transferTo(position, effectiveDstCount, dstChannel);
      assert bytes <= effectiveDstCount;
      //the channel() position has been already moved, sync the file one too
      destination.position(dstPosition + bytes);
      return bytes;
   }

   /**
    * {@see FileChannel#transferFrom}.
    * Do not allow this file to grow beyond {@link #capacity()} and do not change {@link #position()} of the {@code source} file.
    */
   default long transferFrom(TransferableSequentialFile source, long position, long count) throws IOException {
      if (!this.isOpen() || !source.isOpen()) {
         throw new IllegalStateException("both files must be opened!");
      }
      //check available space on source
      final long sourcePosition = source.position();
      final long effectiveSrcCount = effectiveCount(sourcePosition, count, source.size());
      if (effectiveSrcCount <= 0) {
         return 0;
      }
      //check available space on destination === do not allow a file to grow over capacity
      final long dstPosition = position();
      final long effectiveDstCount = effectiveCount(dstPosition, effectiveSrcCount, capacity());
      if (effectiveDstCount <= 0) {
         return 0;
      }
      //prepare the source channel
      final FileChannel sourceChannel = source.channel();
      sourceChannel.position(sourcePosition);
      final long bytes = channel().transferFrom(sourceChannel, position, effectiveDstCount);
      assert bytes <= effectiveDstCount;
      //the source channel position has been already moved, sync the file one too
      source.position(sourcePosition + bytes);
      return bytes;
   }

   /**
    * {@see FileChannel#transferFrom}.
    * Do not allow this file to grow beyond {@link #capacity()} and do not change {@link #position()} of {@code src}.
    */
   default long transferFrom(ReadableByteChannel src, long position, long count) throws IOException {
      if (!this.isOpen()) {
         throw new IllegalStateException("the file must be opened!");
      }
      final long effectiveCount = effectiveCount(position, count, capacity());
      if (effectiveCount <= 0) {
         return 0;
      }
      final long bytes = channel().transferFrom(src, position, effectiveCount);
      assert bytes <= effectiveCount;
      return bytes;
   }

   static long effectiveCount(long position, long count, long limit) {
      final long remaining = limit - position;
      //can't copy more than remaining
      final long effectiveCount = Math.min(count, remaining);
      return effectiveCount;
   }

   /**
    * {@see FileChannel#transferTo}.
    * Do not change {@link #position()} of {@code this} file.
    */
   default long transferTo(long position, long count, WritableByteChannel target) throws IOException {
      if (!this.isOpen()) {
         throw new IllegalStateException("the file must be opened!");
      }
      //prepare the source channel
      final long effectiveCount = effectiveCount(position, count, size());
      if (effectiveCount <= 0) {
         return 0;
      }
      final long bytes = channel().transferTo(position, effectiveCount, target);
      assert bytes <= effectiveCount;
      return bytes;
   }

   @Override
   default void copyTo(SequentialFile newFileName) throws Exception {
      if (newFileName instanceof TransferableSequentialFile) {
         final TransferableSequentialFile file = (TransferableSequentialFile) newFileName;
         TransferableSequentialFile.this.copyTo(file);
      } else {
         SequentialFile.super.copyTo(newFileName);
      }
   }

   default void copyTo(TransferableSequentialFile to) throws Exception {
      try {
         if (!to.isOpen()) {
            to.open();
         }
         if (!isOpen()) {
            open();
         }
         try {
            final long size = this.size();
            final long dstSize = to.size();
            final long dstRemaining = to.capacity() - dstSize;
            if (dstRemaining < size) {
               //TODO boolean ensureCapacity(requiredCapacity) on SequentialFile to partially avoid the exception
               throw new IOException("the destination file is not big enough: remaining " + dstRemaining + " while needed " + size + " bytes");
            }
            //move the position to the end of the file
            to.position(to.size());
            final long bytes = transferTo(0, size, to);
            position(bytes);
            if (bytes < size)
               throw new IOException("copyTo wasn't able to copy all the data: " + bytes + "/" + size + " bytes");
         } finally {
            close();
            to.close();
         }
      } catch (IOException e) {
         factory().onIOError(new ActiveMQIOErrorException(e.getMessage(), e), e.getMessage(), this);
         throw e;
      }
   }

}
