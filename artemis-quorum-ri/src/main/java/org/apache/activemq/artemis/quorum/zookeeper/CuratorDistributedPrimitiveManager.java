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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.activemq.artemis.quorum.DistributedLock;
import org.apache.activemq.artemis.quorum.DistributedPrimitiveManager;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreV2;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryForever;
import org.apache.curator.retry.RetryNTimes;

import static java.util.stream.Collectors.joining;

public class CuratorDistributedPrimitiveManager implements DistributedPrimitiveManager {

   private static final String CONNECT_STRING_PARAM = "connect-string";
   private static final String NAMESPACE_PARAM = "namespace";
   private static final String SESSION_MS_PARAM = "session-ms";
   private static final String SESSION_PERCENT_PARAM = "session-percent";
   private static final String CONNECTION_MS_PARAM = "connection-ms";
   private static final String RETRIES_PARAM = "retries";
   private static final String RETRIES_MS_PARAM = "retries-ms";
   private static final Set<String> VALID_PARAMS = Stream.of(
      CONNECT_STRING_PARAM,
      NAMESPACE_PARAM,
      SESSION_MS_PARAM,
      SESSION_PERCENT_PARAM,
      CONNECTION_MS_PARAM,
      RETRIES_PARAM,
      RETRIES_MS_PARAM).collect(Collectors.toSet());
   private static final String VALID_PARAMS_ON_ERROR = VALID_PARAMS.stream().collect(joining(","));
   private static final String DEFAULT_SESSION_TIMEOUT_MS = Integer.toString(16_000);
   private static final String DEFAULT_CONNECTION_TIMEOUT_MS = Integer.toString(8_000);
   private static final String DEFAULT_RETRIES = Integer.toString(1);
   private static final String DEFAULT_RETRIES_MS = Integer.toString(1000);
   // why 1/3 of the session? https://cwiki.apache.org/confluence/display/CURATOR/TN14
   private static final String DEFAULT_SESSION_PERCENT = Integer.toString(33);

   private static Map<String, String> validateParameters(Map<String, String> config) {
      config.forEach((parameterName, ignore) -> validateParameter(parameterName));
      return config;
   }

   private static void validateParameter(String parameterName) {
      if (!VALID_PARAMS.contains(parameterName)) {
         throw new IllegalArgumentException("non existent parameter " + parameterName + ": accepted list is " + VALID_PARAMS_ON_ERROR);
      }
   }

   private volatile CuratorFramework client;
   private final String connectString;
   private final String namespace;
   private final int sessionMs;
   private final int connectionMs;
   private final int sessionPercent;
   private final RetryPolicy retryPolicy;
   private final Map<String, CuratorDistributedLock> locks;

   public CuratorDistributedPrimitiveManager(Map<String, String> config) {
      this(validateParameters(config), true);
   }

   private CuratorDistributedPrimitiveManager(Map<String, String> config, boolean ignore) {
      this(config.get(CONNECT_STRING_PARAM),
           config.get(NAMESPACE_PARAM),
           Integer.parseInt(config.getOrDefault(SESSION_MS_PARAM, DEFAULT_SESSION_TIMEOUT_MS)),
           Integer.parseInt(config.getOrDefault(SESSION_PERCENT_PARAM, DEFAULT_SESSION_PERCENT)),
           Integer.parseInt(config.getOrDefault(CONNECTION_MS_PARAM, DEFAULT_CONNECTION_TIMEOUT_MS)),
           Integer.parseInt(config.getOrDefault(RETRIES_PARAM, DEFAULT_RETRIES)),
           Integer.parseInt(config.getOrDefault(RETRIES_MS_PARAM, DEFAULT_RETRIES_MS)));
   }

   private CuratorDistributedPrimitiveManager(String connectString,
                                              String namespace,
                                              int sessionMs,
                                              int sessionPercent,
                                              int connectionMs,
                                              int retries,
                                              int retriesMs) {
      Objects.requireNonNull(connectString);
      if (sessionMs <= 0) {
         throw new IllegalArgumentException("session-ms must be > 0");
      }
      if (sessionPercent <= 0) {
         throw new IllegalArgumentException("session-percent must be > 0");
      }
      if (connectionMs <= 0) {
         throw new IllegalArgumentException("connection-ms must be > 0");
      }
      if (retriesMs < 0) {
         throw new IllegalArgumentException("retries-ms must be > 0");
      }
      this.connectString = connectString;
      this.namespace = namespace;
      this.sessionMs = sessionMs;
      this.sessionPercent = sessionPercent;
      this.connectionMs = connectionMs;
      this.locks = new HashMap<>();
      if (retries >= 0) {
         this.retryPolicy = new RetryNTimes(retries, retriesMs);
      } else {
         this.retryPolicy = new RetryForever(retriesMs);
      }
   }

   @Override
   public boolean isStarted() {
      return client != null;
   }

   @Override
   public boolean start(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException {
      if (timeout >= 0) {
         if (timeout > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("curator manager won't support too long timeout ie >" + Integer.MAX_VALUE);
         }
         Objects.requireNonNull(unit);
      }
      if (client != null) {
         return true;
      }
      final CuratorFramework client = CuratorFrameworkFactory.builder()
         .connectString(connectString)
         .namespace(namespace)
         .sessionTimeoutMs(sessionMs)
         .connectionTimeoutMs(connectionMs)
         .retryPolicy(retryPolicy)
         .simulatedSessionExpirationPercent(sessionPercent)
         .build();
      try {
         client.start();
         if (!client.blockUntilConnected((int) timeout, unit)) {
            client.close();
            return false;
         }
         this.client = client;
         return true;
      } catch (InterruptedException e) {
         client.close();
         throw e;
      }
   }

   @Override
   public void start() throws InterruptedException, ExecutionException {
      start(-1, null);
   }

   @Override
   public void stop() {
      final CuratorFramework client = this.client;
      if (client == null) {
         return;
      }
      this.client = null;
      final Listenable<ConnectionStateListener> listenable = client.getConnectionStateListenable();
      locks.forEach((lockId, lock) -> {
         try {
            lock.close(false);
         } catch (Throwable t) {
            // TODO loge?
         } finally {
            listenable.removeListener(lock);
         }
      });
      locks.clear();
      client.close();
   }

   @Override()
   public DistributedLock getDistributedLock(String lockId) {
      Objects.requireNonNull(lockId);
      if (client == null) {
         throw new IllegalStateException("manager isn't started yet!");
      }
      final CuratorDistributedLock lock = locks.get(lockId);
      if (lock != null) {
         return lock;
      }
      final Consumer<CuratorDistributedLock> onCloseLock = closedLock -> {
         final boolean alwaysTrue = locks.remove(closedLock.getLockId(), closedLock);
         assert alwaysTrue;
         client.getConnectionStateListenable().removeListener(closedLock);
      };
      final CuratorDistributedLock newLock = new CuratorDistributedLock(lockId, new InterProcessSemaphoreV2(client, "/" + lockId, 1), onCloseLock);
      client.getConnectionStateListenable().addListener(newLock);
      locks.put(lockId, newLock);
      return newLock;
   }
}
