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
import java.nio.ByteOrder;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.activemq.artemis.api.core.ActiveMQExceptionType;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.io.mapped.batch.SequentialFileWriter;

final class TrackingTimedBufferObserver implements SequentialFileWriter {

   private final int fileSize;
   private final ReentrantReadWriteLock readWriteLock;
   private int lastSyncedPosition;
   private int position;

   TrackingTimedBufferObserver(int fileSize) {
      this.fileSize = fileSize;
      this.lastSyncedPosition = 0;
      this.position = 0;
      this.readWriteLock = new ReentrantReadWriteLock();
   }

   public long fileSize() {
      this.readWriteLock.readLock().lock();
      try {
         return fileSize;
      } finally {
         this.readWriteLock.readLock().unlock();
      }
   }

   public long position() {
      this.readWriteLock.readLock().lock();
      try {
         return position;
      } finally {
         this.readWriteLock.readLock().unlock();
      }
   }

   public int lastSyncedPosition() {
      this.readWriteLock.readLock().lock();
      try {
         return lastSyncedPosition;
      } finally {
         this.readWriteLock.readLock().unlock();
      }
   }

   public boolean isSynced() {
      this.readWriteLock.readLock().lock();
      try {
         return lastSyncedPosition == position;
      } finally {
         this.readWriteLock.readLock().unlock();
      }
   }

   @Override
   public void flushBuffer(ByteBuffer buffer,
                           int index,
                           int length,
                           boolean syncRequested,
                           List<IOCallback> callbacks) {
      this.readWriteLock.writeLock().lock();
      try {
         final int bytes = length;
         final long newPosition = position + bytes;
         if (newPosition > Integer.MAX_VALUE) {
            for (int i = 0; i < callbacks.size(); i++) {
               callbacks.get(i).onError(ActiveMQExceptionType.IO_ERROR.getCode(), "not enough space!");
            }
         } else {
            position = (int) newPosition;
            if (syncRequested) {
               lastSyncedPosition = position;
            }
            for (int i = 0; i < callbacks.size(); i++) {
               callbacks.get(i).done();
            }
         }
      } finally {
         this.readWriteLock.writeLock().unlock();
      }
   }

   @Override
   public void flushBuffer(ByteBuffer buffer, boolean syncRequested, List<IOCallback> callbacks) {
      this.readWriteLock.writeLock().lock();
      try {
         final int bytes = buffer.position();
         final long newPosition = position + bytes;
         if (newPosition > Integer.MAX_VALUE) {
            for (int i = 0; i < callbacks.size(); i++) {
               callbacks.get(i).onError(ActiveMQExceptionType.IO_ERROR.getCode(), "not enough space!");
            }
         } else {
            buffer.flip();
            buffer.position(buffer.limit());
            position = (int) newPosition;
            if (syncRequested) {
               lastSyncedPosition = position;
            }
            for (int i = 0; i < callbacks.size(); i++) {
               callbacks.get(i).done();
            }
         }
      } finally {
         this.readWriteLock.writeLock().unlock();
      }
   }

   @Override
   public int getRemainingBytes() {
      this.readWriteLock.readLock().lock();
      try {
         return Math.min(Integer.MAX_VALUE, fileSize - position);
      } finally {
         this.readWriteLock.readLock().unlock();
      }
   }

   @Override
   public ByteBuffer newBuffer(int size, int limit) {
      final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(size).order(ByteOrder.nativeOrder());
      byteBuffer.limit(limit);
      return byteBuffer;
   }
}
