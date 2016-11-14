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
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledUnsafeDirectByteBufWrapper;
import io.netty.util.internal.PlatformDependent;
import org.apache.activemq.artemis.concurrent.ringbuffer.BytesUtils;
import org.apache.activemq.artemis.core.buffers.impl.ChannelBufferWrapper;
import org.apache.activemq.artemis.core.journal.EncodingSupport;
import org.apache.activemq.artemis.utils.Env;

final class MappedFile implements AutoCloseable {

   private static final int OS_PAGE_SIZE = Env.osPageSize();
   private final MappedByteBuffer buffer;
   private final long address;
   private final UnpooledUnsafeDirectByteBufWrapper byteBufWrapper;
   private final ChannelBufferWrapper channelBufferWrapper;
   private int position;
   private int length;
   private boolean dirty;

   private MappedFile(MappedByteBuffer byteBuffer, int position, int length) throws IOException {
      this.buffer = byteBuffer;
      this.position = position;
      this.length = length;
      this.byteBufWrapper = new UnpooledUnsafeDirectByteBufWrapper();
      this.channelBufferWrapper = new ChannelBufferWrapper(this.byteBufWrapper, false);
      this.address = PlatformDependent.directBufferAddress(buffer);
      this.dirty = false;
   }

   public static MappedFile of(File file, int position, int capacity) throws IOException {
      final MappedByteBuffer buffer;
      final int length;
      try (final RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
         try (FileChannel channel = raf.getChannel()) {
            length = (int) channel.size();
            if (length != capacity && length != 0) {
               throw new IllegalStateException("the file is not " + capacity + " bytes long!");
            }
            buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, capacity);
         }
      }
      return new MappedFile(buffer, position, length);
   }

   public MappedByteBuffer mapped() {
      return buffer;
   }

   public long address() {
      return this.address;
   }

   public void force() {
      if (this.dirty) {
         this.buffer.force();
         this.dirty = false;
      }
   }

   /**
    * Reads a sequence of bytes from this file into the given buffer.
    * <p>
    * <p> Bytes are read starting at this file's specified position.
    */
   public int read(int position, ByteBuf dst, int dstStart, int dstLength) throws IOException {
      final long srcAddress = this.address + position;
      if (dst.hasMemoryAddress()) {
         final long dstAddress = dst.memoryAddress() + dstStart;
         PlatformDependent.copyMemory(srcAddress, dstAddress, dstLength);
      } else if (dst.hasArray()) {
         final byte[] dstArray = dst.array();
         PlatformDependent.copyMemory(srcAddress, dstArray, dstStart, dstLength);
      } else {
         throw new IllegalArgumentException("unsupported byte buffer");
      }
      position += dstLength;
      if (position > this.length) {
         this.length = position;
      }
      return dstLength;
   }

   /**
    * Reads a sequence of bytes from this file into the given buffer.
    * <p>
    * <p> Bytes are read starting at this file's specified position.
    */
   public int read(int position, ByteBuffer dst, int dstStart, int dstLength) throws IOException {
      final long srcAddress = this.address + position;
      if (dst.isDirect()) {
         final long dstAddress = PlatformDependent.directBufferAddress(dst) + dstStart;
         PlatformDependent.copyMemory(srcAddress, dstAddress, dstLength);
      } else {
         final byte[] dstArray = dst.array();
         PlatformDependent.copyMemory(srcAddress, dstArray, dstStart, dstLength);
      }
      position += dstLength;
      if (position > this.length) {
         this.length = position;
      }
      return dstLength;
   }

   /**
    * Reads a sequence of bytes from this file into the given buffer.
    * <p>
    * <p> Bytes are read starting at this file's current position, and
    * then the position is updated with the number of bytes actually read.
    */
   public int read(ByteBuf dst, int dstStart, int dstLength) throws IOException {
      final int remaining = this.length - this.position;
      final int read = Math.min(remaining, dstLength);
      final long srcAddress = this.address + position;
      if (dst.hasMemoryAddress()) {
         final long dstAddress = dst.memoryAddress() + dstStart;
         PlatformDependent.copyMemory(srcAddress, dstAddress, read);
      } else if (dst.hasArray()) {
         final byte[] dstArray = dst.array();
         PlatformDependent.copyMemory(srcAddress, dstArray, dstStart, read);
      } else {
         throw new IllegalArgumentException("unsupported byte buffer");
      }
      position += read;
      return read;
   }

   /**
    * Reads a sequence of bytes from this file into the given buffer.
    * <p>
    * <p> Bytes are read starting at this file's current position, and
    * then the position is updated with the number of bytes actually read.
    */
   public int read(ByteBuffer dst, int dstStart, int dstLength) throws IOException {
      final int remaining = this.length - this.position;
      final int read = Math.min(remaining, dstLength);
      final long srcAddress = this.address + position;
      if (dst.isDirect()) {
         final long dstAddress = PlatformDependent.directBufferAddress(dst) + dstStart;
         PlatformDependent.copyMemory(srcAddress, dstAddress, read);
      } else {
         final byte[] dstArray = dst.array();
         PlatformDependent.copyMemory(srcAddress, dstArray, dstStart, read);
      }
      position += read;
      return read;
   }

   /**
    * Writes an encoded sequence of bytes to this file from the given buffer.
    * <p>
    * <p> Bytes are written starting at this file's current position,
    */
   public void write(EncodingSupport encodingSupport) throws IOException {
      final int encodedSize = encodingSupport.getEncodeSize();
      this.byteBufWrapper.wrap(this.buffer, this.position, encodedSize);
      try {
         encodingSupport.encode(this.channelBufferWrapper);
      } finally {
         this.byteBufWrapper.reset();
      }
      position += encodedSize;
      if (position > this.length) {
         this.length = position;
      }
      this.dirty = true;
   }

   /**
    * Writes a sequence of bytes to this file from the given buffer.
    * <p>
    * <p> Bytes are written starting at this file's current position,
    */
   public void write(ByteBuf src, int srcStart, int srcLength) throws IOException {
      final long destAddress = this.address + position;
      if (src.hasMemoryAddress()) {
         final long srcAddress = src.memoryAddress() + srcStart;
         PlatformDependent.copyMemory(srcAddress, destAddress, srcLength);
      } else if (src.hasArray()) {
         final byte[] srcArray = src.array();
         PlatformDependent.copyMemory(srcArray, srcStart, destAddress, srcLength);
      } else {
         throw new IllegalArgumentException("unsupported byte buffer");
      }
      position += srcLength;
      if (position > this.length) {
         this.length = position;
      }
      this.dirty = true;
   }

   /**
    * Writes a sequence of bytes to this file from the given buffer.
    * <p>
    * <p> Bytes are written starting at this file's current position,
    */
   public void write(ByteBuffer src, int srcStart, int srcLength) throws IOException {
      final long destAddress = this.address + position;
      if (src.isDirect()) {
         final long srcAddress = PlatformDependent.directBufferAddress(src) + srcStart;
         PlatformDependent.copyMemory(srcAddress, destAddress, srcLength);
      } else {
         final byte[] srcArray = src.array();
         PlatformDependent.copyMemory(srcArray, srcStart, destAddress, srcLength);
      }
      position += srcLength;
      if (position > this.length) {
         this.length = position;
      }
      this.dirty = true;
   }

   /**
    * Writes a sequence of bytes to this file from the given buffer.
    * <p>
    * <p> Bytes are written starting at this file's specified position,
    */
   public void write(int position, ByteBuf src, int srcStart, int srcLength) throws IOException {
      final long destAddress = this.address + position;
      if (src.hasMemoryAddress()) {
         final long srcAddress = src.memoryAddress() + srcStart;
         PlatformDependent.copyMemory(srcAddress, destAddress, srcLength);
      } else if (src.hasArray()) {
         final byte[] srcArray = src.array();
         PlatformDependent.copyMemory(srcArray, srcStart, destAddress, srcLength);
      } else {
         throw new IllegalArgumentException("unsupported byte buffer");
      }
      position += srcLength;
      if (position > this.length) {
         this.length = position;
      }
      this.dirty = true;
   }

   /**
    * Writes a sequence of bytes to this file from the given buffer.
    * <p>
    * <p> Bytes are written starting at this file's specified position,
    */
   public void write(int position, ByteBuffer src, int srcStart, int srcLength) throws IOException {
      final long destAddress = this.address + position;
      if (src.isDirect()) {
         final long srcAddress = PlatformDependent.directBufferAddress(src) + srcStart;
         PlatformDependent.copyMemory(srcAddress, destAddress, srcLength);
      } else {
         final byte[] srcArray = src.array();
         PlatformDependent.copyMemory(srcArray, srcStart, destAddress, srcLength);
      }
      position += srcLength;
      if (position > this.length) {
         this.length = position;
      }
      this.dirty = true;
   }

   public void touch(int position) {
      final long pretouchAddress = this.address + position;
      BytesUtils.unsafeCompareAndSwapLong(null, pretouchAddress, 0, 0);
   }

   /**
    * Writes a sequence of bytes to this file from the given buffer.
    * <p>
    * <p> Bytes are written starting at this file's current position,
    */
   public void zeros(int position, final int count) throws IOException {
      //zeroes memory in reverse direction in OS_PAGE_SIZE batches
      //to gain sympathy by the page cache LRU policy
      final long start = this.address + position;
      final long end = start + count;
      int toZeros = count;
      final long lastGap = (int) (end & (OS_PAGE_SIZE - 1));
      final long lastStartPage = end - lastGap;
      long lastZeroed = end;
      if (start <= lastStartPage) {
         if (lastGap > 0) {
            PlatformDependent.setMemory(lastStartPage, lastGap, (byte) 0);
            lastZeroed = lastStartPage;
            toZeros -= lastGap;
         }
      }
      //any that will enter has lastZeroed OS page aligned
      while (toZeros >= OS_PAGE_SIZE) {
         assert BytesUtils.isAligned(lastZeroed, OS_PAGE_SIZE);/**/
         final long startPage = lastZeroed - OS_PAGE_SIZE;
         PlatformDependent.setMemory(startPage, OS_PAGE_SIZE, (byte) 0);
         lastZeroed = startPage;
         toZeros -= OS_PAGE_SIZE;
      }
      //there is anything left in the first OS page?
      if (toZeros > 0) {
         PlatformDependent.setMemory(start, toZeros, (byte) 0);
      }

      position += count;
      if (position > this.length) {
         this.length = position;
      }
      this.dirty = true;
   }

   public int position() {
      return position;
   }

   public void position(int position) {
      this.position = position;
   }

   public long length() {
      return length;
   }

   @Override
   public void close() {
      PlatformDependent.freeDirectBuffer(this.buffer);
      this.dirty = false;
   }
}
