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

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

public class SingleThreadedExecutorTptTest {

   public static void main(String[] args) throws Exception {
      final int capacity = 1024;
      final boolean vanilla = false;
      final int TESTS = 10;
      final int OPERATIONS = 1_000_000;
      final Executor executor = vanilla ? Executors.newSingleThreadExecutor(r -> {
         final Thread t = new Thread(r);
         t.setDaemon(true);
         return t;
      }) : PooledBoundedExecutors.with(1, capacity, Integer.MAX_VALUE);
      final long[] elapsedProductionPerRun = new long[TESTS];
      final long[] elapsedPerRun = new long[TESTS];
      run(executor, new ProducerTask(), TESTS, OPERATIONS, elapsedProductionPerRun, elapsedPerRun);
      for (int t = 0; t < TESTS; t++) {
         System.out.println("TEST\t" + (t + 1));
         System.out.println("\tproduction\t\tservice");
         System.out.println("\t" + (OPERATIONS * 1000_000_000L) / elapsedProductionPerRun[t] + "\t\t" + (OPERATIONS * 1000_000_000L) / elapsedPerRun[t] + "\tops/sec");
      }
      if (executor instanceof AutoCloseable) {
         ((AutoCloseable) executor).close();
      }
   }

   private static void run(Executor executor,
                           ProducerTask task,
                           int tests,
                           int operations,
                           long[] elapsedProductionPerRun,
                           long[] elapsedPerRun) {
      long count = 0;
      for (int t = 0; t < tests; t++) {
         final long start = System.nanoTime();
         for (int i = 0; i < operations; i++) {
            executeOp(executor, task);
            count++;
         }
         final long elapsedProduction = System.nanoTime() - start;
         final AtomicLong lastExecutedCommandId = task.lastExecutedCommandId;
         while (lastExecutedCommandId.get() != count) {
            LockSupport.parkNanos(1L);
         }
         final long elapsed = System.nanoTime() - start;
         elapsedPerRun[t] = elapsed;
         elapsedProductionPerRun[t] = elapsedProduction;
      }
   }

   private static void executeOp(Executor executor, Runnable task) {
      executor.execute(task);
   }

   private static final class ProducerTask implements Runnable {

      private final AtomicLong lastExecutedCommandId = new AtomicLong(0);
      private long commandId;

      @Override
      public void run() {
         commandId++;
         lastExecutedCommandId.lazySet(commandId);
      }
   }

}
