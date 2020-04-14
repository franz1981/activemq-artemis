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

import java.io.File;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.atomix.core.Atomix;
import io.atomix.utils.net.Address;
import org.apache.activemq.artemis.quorum.DistributedPrimitiveManager;
import org.apache.activemq.artemis.quorum.DistributedLock;
import org.jboss.logging.Logger;

public final class AtomixDistributedPrimitiveManager implements DistributedPrimitiveManager {

   private static final Logger LOGGER = Logger.getLogger(AtomixDistributedPrimitiveManager.class);
   private static final String DEFAULT_SESSION_MS = Integer.toString(10_000);
   private static final Duration DEFAULT_GET_LOCK_TIMEOUT = Duration.ofSeconds(4);
   private final File atomixFolder;
   private final Map<String, Address> nodes;
   private final String localMemberId;
   private final Map<String, AtomixDistributedLock> locks;
   private final long sessionMs;
   private Atomix atomix;

   public AtomixDistributedPrimitiveManager(Map<String, String> args) {
      this(args.get("id"),
           new File(args.get("data-dir"),
           args.getOrDefault("folder", "atomix")),
           AtomixFactory.parseNodes(args.get("nodes"), ","),
           Long.parseLong(args.getOrDefault("session-ms", DEFAULT_SESSION_MS)));
   }

   public AtomixDistributedPrimitiveManager(String localMemberId, File atomixFolder, Map<String, Address> nodes) {
      this(localMemberId, atomixFolder, nodes, Long.parseLong(DEFAULT_SESSION_MS));
   }

   public AtomixDistributedPrimitiveManager(String localMemberId,
                                            File atomixFolder,
                                            Map<String, Address> nodes,
                                            long sessionMs) {
      this.atomixFolder = atomixFolder;
      this.nodes = new HashMap<>(nodes);
      this.localMemberId = localMemberId;
      this.atomix = null;
      this.locks = new HashMap<>();
      this.sessionMs = sessionMs;
      Objects.requireNonNull(atomixFolder);
      if (nodes.isEmpty()) {
         throw new IllegalArgumentException("nodes cannot be empty");
      }
      if (!nodes.containsKey(localMemberId)) {
         throw new IllegalArgumentException("nodes must contains id");
      }
      if (sessionMs <= 0) {
         throw new IllegalArgumentException("session-ms must be > 0");
      }
   }

   private static void silentSyncStopAtomix(Atomix atomix) {
      try {
         atomix.stop().join();
      } catch (Throwable t) {
         // ignore this, it's a best effort op to cleanup it
      }
   }

   @Override
   public boolean isStarted() {
      return this.atomix != null;
   }

   @Override
   public boolean start(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException {
      if (timeout >= 0) {
         Objects.requireNonNull(unit);
      }
      if (this.atomix != null) {
         return true;
      }
      final Atomix atomix = AtomixFactory.createAtomix(localMemberId, atomixFolder, nodes);
      final CompletableFuture<Void> start = atomix.start();
      try {
         if (timeout > 0) {
            start.get(timeout, unit);
         } else {
            start.get();
         }
         this.atomix = atomix;
         LOGGER.info("started");
         return true;
      } catch (InterruptedException e) {
         silentSyncStopAtomix(atomix);
         throw e;
      } catch (TimeoutException e) {
         silentSyncStopAtomix(atomix);
         return false;
      }
   }

   @Override
   public void start() throws InterruptedException, ExecutionException {
      start(-1, null);
   }

   @Override
   public void stop() {
      final Atomix atomix = this.atomix;
      if (atomix != null) {
         this.atomix = null;
         locks.forEach((lockId, lock) -> {
            try {
               lock.close(false);
            } catch (Throwable t) {
               LOGGER.debugf(t, "Failed to close %s lock", lockId);
            }
         });
         locks.clear();
         silentSyncStopAtomix(atomix);
         LOGGER.info("stopped");
      }
   }

   @Override
   public DistributedLock getDistributedLock(String lockId) throws InterruptedException, ExecutionException, TimeoutException {
      final Atomix atomix = this.atomix;
      if (atomix == null) {
         throw new IllegalStateException("the manager isn't started");
      }
      final AtomixDistributedLock lock = locks.get(lockId);
      if (lock != null) {
         return lock;
      }
      final AtomixDistributedLock newLock;
      newLock = AtomixDistributedLock.with(locks::remove, atomix, lockId, Duration.ofMillis(sessionMs))
         .get(DEFAULT_GET_LOCK_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
      locks.put(lockId, newLock);
      return newLock;
   }
}
