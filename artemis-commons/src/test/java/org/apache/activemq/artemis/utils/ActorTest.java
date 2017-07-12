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

package org.apache.activemq.artemis.utils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import org.apache.activemq.artemis.utils.actors.Actor;
import org.apache.activemq.artemis.utils.actors.OrderedExecutor;

public final class ActorTest {

   public static void main(String[] args) {
      final int TESTS = 4;
      final int MESSAGES = 10_000_000;
      final ExecutorService executor = Executors.newSingleThreadExecutor();
      final OrderedExecutor orderedExecutor = new OrderedExecutor(executor);
      final AtomicInteger test = new AtomicInteger();
      final Actor<Boolean> actor = new Actor<>(orderedExecutor, m -> test.incrementAndGet());
      for (int t = 0; t < TESTS; t++) {
         test.lazySet(0);
         final long start = System.nanoTime();
         for (int m = 0; m < MESSAGES; m++) {
            actor.act(Boolean.TRUE);
         }
         while (test.get() < MESSAGES) {
            LockSupport.parkNanos(1L);
         }
         final long elapsed = System.nanoTime() - start;
         System.out.println(MESSAGES * 1000_000_000L / elapsed + " msg/sec");
      }
      executor.shutdown();
   }

}
