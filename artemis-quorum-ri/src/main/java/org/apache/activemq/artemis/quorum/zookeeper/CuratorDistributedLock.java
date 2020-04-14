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
package org.apache.activemq.artemis.quorum.zookeeper;

import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.activemq.artemis.quorum.DistributedLock;
import org.apache.activemq.artemis.quorum.UnavailableStateException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreV2;
import org.apache.curator.framework.recipes.locks.Lease;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;

final class CuratorDistributedLock implements DistributedLock, ConnectionStateListener {

   private final String lockId;
   private final InterProcessSemaphoreV2 ipcSem;
   private final Consumer<CuratorDistributedLock> onClose;
   private final CopyOnWriteArrayList<LockListener> listeners;
   private Lease lease;
   private boolean unavailable;
   private boolean closed;
   private byte[] leaseVersion;

   CuratorDistributedLock(String lockId, InterProcessSemaphoreV2 ipcSem, Consumer<CuratorDistributedLock> onClose) {
      this.lockId = lockId;
      this.ipcSem = ipcSem;
      this.onClose = onClose;
      this.listeners = new CopyOnWriteArrayList<>();
      this.closed = false;
      this.unavailable = false;
      this.leaseVersion = null;
   }

   @Override
   public void stateChanged(CuratorFramework client, ConnectionState newState) {

      synchronized (this) {
         if (closed) {
            return;
         }
         if (unavailable) {
            return;
         }
         if (newState == ConnectionState.RECONNECTED && leaseVersion != null) {
            assert lease != null;
            try {
               if (Arrays.equals(lease.getData(), leaseVersion)) {
                  return;
               }
               newState = ConnectionState.LOST;
            } catch (Exception e) {
               newState = ConnectionState.LOST;
            }
         }
         if (newState != ConnectionState.LOST) {
            return;
         }
         lease = null;
         leaseVersion = null;
         unavailable = true;
         // TODO maybe put it outside the synchronized block or document that it cannot block
         for (LockListener listener : listeners) {
            listener.stateChanged(LockListener.EventType.UNAVAILABLE);
         }
      }
   }

   @Override
   public String getLockId() {
      return lockId;
   }

   private void checkNotClosed() {
      if (closed) {
         throw new IllegalStateException("This lock is closed");
      }
   }

   @Override
   public synchronized boolean isHeldByCaller() throws UnavailableStateException {
      checkNotClosed();
      if (unavailable) {
         throw new UnavailableStateException(lockId + " lock state isn't available");
      }
      if (lease == null) {
         return false;
      }
      assert leaseVersion != null;
      try {
         return Arrays.equals(lease.getData(), leaseVersion);
      } catch (Throwable t) {
         throw new UnavailableStateException(t);
      }
   }

   @Override
   public synchronized boolean tryLock() throws UnavailableStateException {
      checkNotClosed();
      if (lease != null) {
         throw new IllegalStateException("unlock first");
      }
      if (unavailable) {
         throw new UnavailableStateException(lockId + " lock state isn't available");
      }
      try {
         final byte[] leaseVersion = UUID.randomUUID().toString().getBytes();
         ipcSem.setNodeData(leaseVersion);
         lease = ipcSem.acquire(0, TimeUnit.NANOSECONDS);
         if (lease == null) {
            ipcSem.setNodeData(null);
            return false;
         }
         this.leaseVersion = leaseVersion;
         assert Arrays.equals(lease.getData(), leaseVersion);
         return true;
      } catch (Throwable e) {
         throw new UnavailableStateException(e);
      }
   }

   @Override
   public synchronized void unlock() throws UnavailableStateException {
      checkNotClosed();
      if (unavailable) {
         throw new UnavailableStateException(lockId + " lock state isn't available");
      }
      final Lease lease = this.lease;
      if (lease != null) {
         this.lease = null;
         this.leaseVersion = null;
         try {
            ipcSem.returnLease(lease);
         } catch (Throwable e) {
            throw new UnavailableStateException(e);
         }
      }
   }

   @Override
   public synchronized void addListener(LockListener listener) {
      checkNotClosed();
      listeners.add(listener);
   }

   @Override
   public synchronized void removeListener(LockListener listener) {
      checkNotClosed();
      listeners.remove(listener);
   }

   public synchronized void close(boolean useCallback) {
      if (closed) {
         return;
      }
      closed = true;
      listeners.clear();
      if (useCallback) {
         onClose.accept(this);
      }
      final Lease lease = this.lease;
      if (lease == null) {
         return;
      }
      this.lease = null;
      if (unavailable) {
         return;
      }
      try {
         ipcSem.returnLease(lease);
      } catch (Throwable t) {
         // TODO silent, but debug ;)
      }
   }

   @Override
   public void close() {
      close(true);
   }
}
