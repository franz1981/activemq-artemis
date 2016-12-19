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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

public class ExecutorFactoryTptTest {

   public static void main(String[] args) throws Exception {
      final int producers = 1;
      final int consumers = 1;
      final int capacity = 1024 * 4;
      final int parentCapacity = 1024;
      final ExecutorType executorType = ExecutorType.ORDERED;
      final int TESTS = 5;
      final int OPERATIONS = 1_000_000;
      final Executor executor = PooledBoundedExecutors.with(consumers, parentCapacity, Integer.MAX_VALUE);
      final Executor producerExecutor;
      switch (executorType) {
         case ORDERED:
            producerExecutor = new OrderedExecutorFactory(executor).getExecutor();
            break;
         case BOUNDED_ORDERED:
            producerExecutor = new BoundedOrderedExecutorFactory(executor, capacity, Integer.MAX_VALUE).getExecutor();
            break;
         default:
            throw new AssertionError("not supported case!");
      }
      final CountDownLatch[] producersStart = new CountDownLatch[TESTS];
      final long[][] elapsedProducingPerProducer = new long[producers][TESTS];
      final long[][] elapsedPerProducer = new long[producers][TESTS];
      final CountDownLatch[] producersStop = new CountDownLatch[TESTS];
      for (int i = 0; i < TESTS; i++) {
         producersStart[i] = new CountDownLatch(producers);
         producersStop[i] = new CountDownLatch(producers);
      }
      final Thread[] producerThreads = new Thread[producers];
      for (int i = 0; i < producers; i++) {
         elapsedPerProducer[i] = new long[TESTS];
         elapsedProducingPerProducer[i] = new long[TESTS];
         final int producerIndex = i;
         final Thread producerRunner = new Thread(() -> run(producerExecutor, new ProducerTask(), TESTS, OPERATIONS, elapsedProducingPerProducer[producerIndex], elapsedPerProducer[producerIndex], producersStart, producersStop));
         producerRunner.start();
         producerThreads[i] = producerRunner;
      }

      for (int i = 0; i < producers; i++) {
         producerThreads[i].join();
      }
      if (executor instanceof AutoCloseable) {
         ((AutoCloseable) executor).close();
      }
      for (int t = 0; t < TESTS; t++) {
         System.out.println("TEST\t" + (t + 1));
         System.out.println("\tproduction\t\tservice");
         for (int p = 0; p < producers; p++) {
            System.out.println(p + ":\t" + (OPERATIONS * 1000_000_000L) / elapsedProducingPerProducer[p][t] + "\t\t" + (OPERATIONS * 1000_000_000L) / elapsedPerProducer[p][t] + "\tops/sec");
         }
      }
   }

   private static void run(Executor executor,
                           ProducerTask task,
                           int tests,
                           int operations,
                           long[] elapsedProductionPerRun,
                           long[] elapsedPerRun,
                           CountDownLatch[] startedPerRun,
                           CountDownLatch[] finishedPerRun) {
      long count = 0;

      for (int t = 0; t < tests; t++) {
         final CountDownLatch started = startedPerRun[t];
         started.countDown();
         try {
            started.await();
         } catch (Exception e) {
            //no_op
         }
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
         //wait the other producers
         final CountDownLatch finishedRun = finishedPerRun[t];
         finishedRun.countDown();
         try {
            finishedRun.await();
         } catch (Exception e) {
            //no_op
         }
      }
   }

   private static void executeOp(Executor executor, Runnable task) {
      executor.execute(task);
   }

   private enum ExecutorType {
      ORDERED, BOUNDED_ORDERED
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
