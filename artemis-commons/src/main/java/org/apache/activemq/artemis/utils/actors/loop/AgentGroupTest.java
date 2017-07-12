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

package org.apache.activemq.artemis.utils.actors.loop;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

public class AgentGroupTest {

   public static void main(String[] args) {
      final int TESTS = 5;
      final int MESSAGES = 1000_000;
      final int SUBSCRIPTIONS = 1;
      final ExecutorService executor = Executors.newCachedThreadPool();
      try (AgentManager agentManager = new AgentProcessor(new BlockingWaitStrategy())) {
         executor.submit(agentManager);
         final List<Future<?>> producers = new ArrayList<>(SUBSCRIPTIONS);
         for (int s = 0; s < SUBSCRIPTIONS; s++) {
            final int index = s;
            final Future<?> finished = executor.submit(() -> {
               final AtomicInteger test = new AtomicInteger();
               try (AgentSubscription<Boolean> agentSubscription = agentManager.registerExclusive(e -> test.incrementAndGet())) {
                  for (int t = 0; t < TESTS; t++) {
                     test.lazySet(0);
                     final long start = System.nanoTime();
                     for (int m = 0; m < MESSAGES; m++) {
                        agentSubscription.trySubmit(Boolean.TRUE);
                     }
                     while (test.get() < MESSAGES) {
                        LockSupport.parkNanos(1L);
                     }
                     final long elapsed = System.nanoTime() - start;
                     System.out.println("[" + (index + 1) + "] " + MESSAGES * 1000_000_000L / elapsed + " msg/sec");
                  }
               }
            });
            producers.add(finished);
         }
         producers.forEach(f -> {
            try {
               f.get();
            } catch (ExecutionException | InterruptedException e) {
               e.printStackTrace();
            }
         });
      }
      executor.shutdown();
   }
}
