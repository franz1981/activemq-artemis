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
package org.apache.activemq.artemis.quorum;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public interface DistributedLock extends AutoCloseable {

   String getLockId();

   boolean isHeldByCaller() throws UnavailableStateException;

   boolean tryLock() throws UnavailableStateException;

   default boolean tryLock(long timeout, TimeUnit unit) throws UnavailableStateException, InterruptedException {
      final long LINGER_NS = TimeUnit.MILLISECONDS.toNanos(100);
      if (timeout < 0) {
         throw new IllegalArgumentException("timeout cannot be negative");
      }
      Objects.requireNonNull(unit);
      if (timeout == 0) {
         return tryLock();
      }
      final long timeoutNs = unit.toNanos(timeout);
      final long start = System.nanoTime();
      while (!Thread.currentThread().isInterrupted()) {
         final long startTryLock = System.nanoTime();
         if (tryLock()) {
            return true;
         }
         final long now = System.nanoTime();
         final long elapsed = now - start;
         if (elapsed >= timeoutNs) {
            return false;
         }
         final long tryLockDuration = now - startTryLock;
         final long expectedDistanceFromNextTry = Math.max(LINGER_NS, tryLockDuration);
         final long nextElapsed = elapsed + expectedDistanceFromNextTry;
         if (nextElapsed > timeoutNs) {
            // trying take too much time, let's timeout now
            return false;
         }
         final long nanosToWait = LINGER_NS - tryLockDuration;
         if (nanosToWait > 0) {
            LockSupport.parkNanos(nanosToWait);
         }
      }
      throw new InterruptedException();
   }

   void unlock() throws UnavailableStateException;

   void addListener(LockListener listener);

   void removeListener(LockListener listener);

   interface LockListener {

      enum EventType {
         UNAVAILABLE
      }

      void stateChanged(EventType eventType);

   }

   @Override
   void close();
}
