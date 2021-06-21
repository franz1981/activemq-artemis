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
package org.apache.activemq.artemis.quorum.file;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;

import org.apache.activemq.artemis.quorum.DistributedLock;
import org.apache.activemq.artemis.quorum.UnavailableStateException;

final class FileDistributedLock implements DistributedLock {

   private static final int HOLDER_ID_BYTES_LENGTH = UUID.randomUUID().toString().getBytes().length;
   private static final byte[] NO_HOLDER = new byte[HOLDER_ID_BYTES_LENGTH];
   private final String lockId;
   private final Consumer<String> onClosedLock;
   private boolean closed;
   private FileLock fileLock;
   private final FileChannel channel;

   FileDistributedLock(Consumer<String> onClosedLock, File locksFolder, String lockId) throws IOException {
      this.onClosedLock = onClosedLock;
      this.lockId = lockId;
      this.closed = false;
      this.fileLock = null;
      this.channel = FileChannel.open(new File(locksFolder, lockId).toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
   }

   private void checkNotClosed() {
      if (closed) {
         throw new IllegalStateException("This lock is closed");
      }
   }

   @Override
   public Optional<String> version() throws UnavailableStateException {
      checkNotClosed();
      try {
         if (channel.size() < 0) {
            return Optional.empty();
         }
      } catch (IOException ioException) {
         throw new UnavailableStateException(ioException);
      }
      final byte[] holderId = new byte[HOLDER_ID_BYTES_LENGTH];
      try {
         channel.read(ByteBuffer.wrap(holderId), 0);
         if (Arrays.equals(holderId, NO_HOLDER)) {
            return Optional.empty();
         }
         return Optional.of(new String(holderId));
      } catch (IOException ioException) {
         throw new UnavailableStateException(ioException);
      }
   }

   @Override
   public String getLockId() {
      checkNotClosed();
      return lockId;
   }

   @Override
   public boolean isHeldByCaller() {
      checkNotClosed();
      final FileLock fileLock = this.fileLock;
      if (fileLock == null) {
         return false;
      }
      return fileLock.isValid();
   }

   @Override
   public boolean tryLock() {
      checkNotClosed();
      final FileLock fileLock = this.fileLock;
      if (fileLock != null) {
         throw new IllegalStateException("unlock first");
      }
      final FileLock lock;
      try {
         lock = channel.tryLock();
      } catch (OverlappingFileLockException o) {
         // this process already hold this lock, but not this manager
         return false;
      } catch (Throwable t) {
         throw new IllegalStateException(t);
      }
      if (lock == null) {
         return false;
      }
      this.fileLock = lock;
      try {
         channel.write(ByteBuffer.wrap(UUID.randomUUID().toString().getBytes()), 0);
         return true;
      } catch (IOException ioException) {
         try {
            this.fileLock.release();
         } catch (IOException e) {
            // no-nop
         } finally {
            this.fileLock = null;
         }
         return false;
      }
   }

   @Override
   public void unlock() {
      checkNotClosed();
      final FileLock fileLock = this.fileLock;
      if (fileLock != null) {
         this.fileLock = null;
         try {
            channel.write(ByteBuffer.wrap(NO_HOLDER), 0);
         } catch (IOException ie) {
            // it's a problem! stale holder
         }
         try {
            fileLock.close();
         } catch (IOException e) {
            // noop
         }
      }
   }

   @Override
   public void addListener(LockListener listener) {
      checkNotClosed();
      // noop
   }

   @Override
   public void removeListener(LockListener listener) {
      checkNotClosed();
      // noop
   }

   public boolean isClosed() {
      return closed;
   }

   public void close(boolean useCallback) {
      if (closed) {
         return;
      }
      try {
         if (useCallback) {
            onClosedLock.accept(lockId);
         }
         unlock();
         channel.close();
      } catch (IOException e) {
         // ignore it
      } finally {
         closed = true;
      }
   }

   @Override
   public void close() {
      close(true);
   }
}
