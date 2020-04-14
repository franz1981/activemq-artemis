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
package org.apache.activemq.artemis.quorum.atomix;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import io.atomix.core.Atomix;
import io.atomix.core.lock.AtomicLock;
import io.atomix.primitive.PrimitiveState;
import io.atomix.utils.time.Version;
import org.apache.activemq.artemis.quorum.DistributedLock;
import org.apache.activemq.artemis.quorum.UnavailableStateException;
import org.jboss.logging.Logger;

final class AtomixDistributedLock implements DistributedLock {

   private static final Logger LOGGER = Logger.getLogger(AtomixDistributedLock.class);
   private final Consumer<String> onClosedLock;
   private final AtomicLock atomicLock;
   private final String lockId;
   private final CopyOnWriteArrayList<LockListener> listeners;
   private boolean closed;
   private final Consumer<PrimitiveState> primitiveStateListener;
   private Version lockVersion;

   public static CompletableFuture<AtomixDistributedLock> with(Consumer<String> onClosedLock,
                                                               Atomix atomix,
                                                               String lockId,
                                                               Duration sessionTimeout) {
      return AtomixFactory.createAtomicLock(atomix, lockId, sessionTimeout).thenApply(atomicLock -> new AtomixDistributedLock(onClosedLock, atomicLock, lockId));
   }

   private AtomixDistributedLock(Consumer<String> onClosedLock, AtomicLock atomicLock, String lockId) {
      this.onClosedLock = onClosedLock;
      this.atomicLock = atomicLock;
      this.lockId = lockId;
      this.closed = false;
      this.listeners = new CopyOnWriteArrayList<>();
      this.primitiveStateListener = this::onStateChanged;
      this.atomicLock.async().addStateChangeListener(primitiveStateListener);
      this.lockVersion = null;
   }

   private void onStateChanged(PrimitiveState state) {
      LOGGER.info(state);
      switch (state) {
         case SUSPENDED:
         case EXPIRED:
         case CLOSED:
            for (LockListener listener : listeners) {
               listener.stateChanged(LockListener.EventType.UNAVAILABLE);
            }
            break;
      }
   }

   public long version() {
      checkNotClosed();
      return this.lockVersion == null ? Long.MAX_VALUE : lockVersion.value();
   }

   private void checkNotClosed() {
      if (closed) {
         throw new IllegalStateException("The election is closed");
      }
   }

   @Override
   public String getLockId() {
      checkNotClosed();
      return lockId;
   }

   @Override
   public boolean isHeldByCaller() throws UnavailableStateException {
      if (lockVersion == null) {
         return false;
      }
      try {
         return this.atomicLock.isLocked(lockVersion);
      } catch (Throwable t) {
         throw new UnavailableStateException(t);
      }
   }

   @Override
   public boolean tryLock() throws UnavailableStateException {
      if (this.lockVersion != null) {
         throw new IllegalStateException("unlock first");
      }
      try {
         final Optional<Version> version = this.atomicLock.tryLock();
         if (!version.isPresent()) {
            LOGGER.infof("Failed to acquire lock %s", lockId);
            return false;
         }
         this.lockVersion = version.get();
         LOGGER.infof("Acquired lock %s with version %s", lockId, lockVersion);
         return true;
      } catch (Throwable t) {
         throw new UnavailableStateException(t);
      }
   }

   @Override
   public void unlock() throws UnavailableStateException {
      if (lockVersion != null) {
         lockVersion = null;
         try {
            atomicLock.unlock();
         } catch (Throwable t) {
            throw new UnavailableStateException(t);
         }
      }
   }

   @Override
   public void addListener(LockListener listener) {
      checkNotClosed();
      listeners.add(listener);
   }

   @Override
   public void removeListener(LockListener listener) {
      checkNotClosed();
      listeners.remove(listener);
   }

   @Override
   public void close() {
      close(true);
   }

   protected void close(boolean useCallback) {
      if (closed) {
         return;
      }
      closed = true;
      if (useCallback) {
         onClosedLock.accept(lockId);
      }
      listeners.clear();
      atomicLock.async().removeStateChangeListener(primitiveStateListener);
      try {
         if (lockVersion != null) {
            lockVersion = null;
            try {
               atomicLock.unlock();
            } catch (Throwable t) {
               LOGGER.debugf(t, "Atomix lock %s cannot unlock", lockId);
            }
         }
      } finally {
         atomicLock.close();
      }
   }

}
