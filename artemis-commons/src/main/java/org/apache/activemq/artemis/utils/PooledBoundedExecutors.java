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
import java.util.concurrent.locks.LockSupport;

import io.netty.util.internal.shaded.org.jctools.queues.MessagePassingQueue;
import io.netty.util.internal.shaded.org.jctools.queues.MpscArrayQueue;

public final class PooledBoundedExecutors {

   private PooledBoundedExecutors() {

   }

   public static CloseableBoundedExecutor with(int consumerThreads, int capacity, int maxBurstSize) {
      if (consumerThreads == 1) {
         return new SingleConsumerExecutor(capacity, maxBurstSize).start();
      } else {
         return new MultiConsumerExecutor(consumerThreads, capacity, maxBurstSize).start();
      }
   }

   private static int doWorkOrWait(ConsumerTask consumerTask, int waitCount) {
      if (consumerTask.doWork() == 0) {
         if (waitCount < 100) {
            waitCount++;
            //Busy Spin
         } else if (waitCount < 200) {
            waitCount++;
            Thread.yield();
         } else {
            LockSupport.parkNanos(1L);
         }
      } else {
         waitCount = 0;
      }
      return waitCount;
   }

   private static final class SingleConsumerExecutor implements CloseableBoundedExecutor {

      private final Thread runner;
      private final CountDownLatch started;
      private final MessagePassingQueue<Runnable> commands;

      SingleConsumerExecutor(final int capacity, final int maxBurstSize) {
         this.started = new CountDownLatch(1);
         this.commands = new MpscArrayQueue<>(capacity);
         this.runner = new Thread(() -> {
            final ConsumerTask consumerTask = new ConsumerTask(this.commands, maxBurstSize);
            started.countDown();
            final Thread currentThread = Thread.currentThread();
            int count = 0;
            while (!currentThread.isInterrupted()) {
               count = doWorkOrWait(consumerTask, count);
            }
         });
      }

      @Override
      public int pendingTasks() {
         return this.commands.size();
      }

      @Override
      public boolean isEmpty() {
         return this.commands.isEmpty();
      }

      @Override
      public int capacity() {
         return this.commands.capacity();
      }

      @Override
      public int remainingCapacity() {
         return (this.commands.capacity() - this.commands.size());
      }

      @Override
      public boolean tryExecute(Runnable command) {
         final Thread producerThread = Thread.currentThread();
         if (producerThread == runner) {
            command.run();
            return true;
         } else {
            return commands.relaxedOffer(command);
         }
      }

      @Override
      public void execute(Runnable command) {
         final Thread producerThread = Thread.currentThread();
         if (producerThread == runner) {
            command.run();
         } else {
            while (!commands.offer(command)) {
               if (producerThread.isInterrupted()) {
                  throw new IllegalStateException("can't execute the command; interrupted producer thread!");
               }
               LockSupport.parkNanos(1L);
            }
         }
      }

      SingleConsumerExecutor start() {
         try {
            runner.start();
            started.await();
         } catch (InterruptedException e) {
            //NO_OP?
         } finally {
            ///could drains all the messages to avoid leaks?
         }
         return this;
      }

      @Override
      public void close() {
         runner.interrupt();
         try {
            runner.join();
         } catch (InterruptedException e) {
            //NO_OP?
         }
      }
   }

   private static final class MultiConsumerExecutor implements CloseableBoundedExecutor {

      private final Thread[] runners;
      private final CountDownLatch started;
      private final ThreadLocal<Thread> executorThreads;
      private final MessagePassingQueue<Runnable>[] commands;

      MultiConsumerExecutor(int poolSize, final int capacity, final int maxBurstSize) {
         this.runners = new Thread[poolSize];
         this.started = new CountDownLatch(poolSize);
         this.executorThreads = new ThreadLocal<>();
         this.commands = new MessagePassingQueue[poolSize];
         for (int i = 0; i < poolSize; i++) {
            final MessagePassingQueue<Runnable> consumerCommands = new MpscArrayQueue<>(capacity);
            commands[i] = consumerCommands;
            this.runners[i] = new Thread(() -> {
               final ConsumerTask consumerTask = new ConsumerTask(consumerCommands, maxBurstSize);
               started.countDown();
               final Thread currentThread = Thread.currentThread();
               this.executorThreads.set(currentThread);
               try {
                  int count = 0;
                  while (!currentThread.isInterrupted()) {
                     count = doWorkOrWait(consumerTask, count);
                  }
               } finally {
                  this.executorThreads.set(null);
               }
            });
         }
      }

      @Override
      public boolean tryExecute(Runnable command) {
         final Thread producerThread = Thread.currentThread();
         if (producerThread == this.executorThreads.get()) {
            command.run();
            return true;
         } else {
            //round-robin first free commands queue
            for (MessagePassingQueue<Runnable> cmds : commands) {
               if (cmds.relaxedOffer(command)) {
                  return true;
               }
            }
            return false;
         }
      }

      @Override
      public int pendingTasks() {
         long pendingTasks = 0;
         for (MessagePassingQueue<Runnable> cmds : commands) {
            pendingTasks += cmds.size();
         }
         return (int) Math.min(Integer.MAX_VALUE, pendingTasks);
      }

      @Override
      public int capacity() {
         long capacity = 0;
         for (MessagePassingQueue<Runnable> cmds : commands) {
            capacity += cmds.capacity();
         }
         return (int) Math.min(Integer.MAX_VALUE, capacity);
      }

      @Override
      public int remainingCapacity() {
         long remainingCapacity = 0;
         for (MessagePassingQueue<Runnable> cmds : commands) {
            remainingCapacity += (cmds.capacity() - cmds.size());
         }
         return (int) Math.min(Integer.MAX_VALUE, remainingCapacity);
      }

      @Override
      public boolean isEmpty() {
         for (MessagePassingQueue<Runnable> cmds : commands) {
            if (!cmds.isEmpty()) {
               return false;
            }
         }
         return true;
      }

      @Override
      public void execute(Runnable command) {
         final Thread producerThread = Thread.currentThread();
         if (producerThread == this.executorThreads.get()) {
            command.run();
         } else {
            //round-robin first free commands queue
            do {
               for (MessagePassingQueue<Runnable> cmds : commands) {
                  if (cmds.offer(command)) {
                     return;
                  }
               }
               LockSupport.parkNanos(1L);
            } while (!producerThread.isInterrupted());
            throw new IllegalStateException("can't execute the command; interrupted producer thread!");
         }
      }

      MultiConsumerExecutor start() {
         for (int i = 0; i < runners.length; i++) {
            runners[i].start();
         }
         try {
            started.await();
         } catch (InterruptedException e) {
            //NO_OP?
         } finally {
            ///could drains all the messages to avoid leaks?
         }
         return this;
      }

      @Override
      public void close() {
         for (int i = 0; i < runners.length; i++) {
            runners[i].interrupt();
         }
         for (int i = 0; i < runners.length; i++) {
            try {
               runners[i].join();
            } catch (InterruptedException e) {
               //NO_OP?
            }
         }
      }
   }

   private static final class ConsumerTask {

      private final MessagePassingQueue<Runnable> commands;
      private final int maxBurstSize;

      ConsumerTask(MessagePassingQueue<Runnable> commands, int maxBurstSize) {
         this.maxBurstSize = maxBurstSize;
         this.commands = commands;
      }

      private static int drainMessages(final MessagePassingQueue<Runnable> commands, int maxBurstSize) {
         for (int i = 0; i < maxBurstSize; i++) {
            final Runnable command = commands.poll();
            if (command == null) {
               return i;
            }
            command.run();
         }
         return maxBurstSize;
      }

      public int doWork() {
         return drainMessages(this.commands, this.maxBurstSize);
      }
   }

}
