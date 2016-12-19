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
package org.apache.activemq.artemis.utils;

import java.util.concurrent.Executor;
import java.util.concurrent.locks.LockSupport;

import io.netty.util.internal.shaded.org.jctools.queues.MessagePassingQueue;
import io.netty.util.internal.shaded.org.jctools.queues.MpscArrayQueue;
import io.netty.util.internal.shaded.org.jctools.util.UnsafeAccess;
import org.apache.activemq.artemis.api.core.ActiveMQInterruptedException;
import org.jboss.logging.Logger;

/**
 * A factory for producing executors that run all tasks in order, which delegate to a single common executor instance.
 */
public final class BoundedOrderedExecutorFactory implements ExecutorFactory {

   private static final int DEFAULT_CAPACITY = Integer.getInteger("bound.executor.capacity", 1024 * 16);
   private static final Logger logger = Logger.getLogger(BoundedOrderedExecutorFactory.class);

   private final Executor parent;
   private final int capacity;
   private final int maxBurstSize;

   /**
    * Construct a new instance delegating to the given parent executor.
    *
    * @param parent the parent executor
    */
   public BoundedOrderedExecutorFactory(Executor parent) {
      this(parent, DEFAULT_CAPACITY, DEFAULT_CAPACITY);
   }

   /**
    * Construct a new instance delegating to the given parent executor.
    *
    * @param parent the parent executor
    */
   public BoundedOrderedExecutorFactory(Executor parent, int capacity, int maxBurstSize) {
      this.parent = parent;
      this.capacity = capacity;
      this.maxBurstSize = maxBurstSize;
   }

   private static int drainCommands(final MessagePassingQueue<Runnable> commands, int maxBurstSize) {
      for (int i = 0; i < maxBurstSize; i++) {
         final Runnable command = commands.poll();
         if (command == null) {
            return i;
         }
         try {
            command.run();
         } catch (ActiveMQInterruptedException e) {
            // This could happen during shutdowns. Nothing to be concerned about here
            logger.debug("Interrupted Thread", e);
         } catch (Throwable t) {
            logger.warn(t.getMessage(), t);
         }
      }
      return maxBurstSize;
   }

   /**
    * Get an executor that always executes tasks in order.
    *
    * @return an ordered executor
    */
   @Override
   public BoundedExecutor getExecutor() {

      return getExecutor(this.capacity, this.maxBurstSize);
   }

   public BoundedExecutor getExecutor(int capacity, int maxBurstSize) {
      return new BoundedOrderedExecutor(new MpscArrayQueue<>(capacity), maxBurstSize, parent);
   }

   private abstract static class BoundedOrderedExecutorL0Pad {

      protected long p00, p01, p02, p03, p04, p05, p06;
      protected long p10, p11, p12, p13, p14, p15, p16, p17;
   }

   private abstract static class OrderedExecutorState extends BoundedOrderedExecutorL0Pad {

      protected static final int RELEASED = 0;
      protected static final long STATE_OFFSET;

      static {
         try {
            STATE_OFFSET = UnsafeAccess.UNSAFE.objectFieldOffset(OrderedExecutorState.class.getDeclaredField("state"));
         } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
         }
      }

      private final int state = 0;

   }

   private abstract static class BoundedOrderedExecutorL1Pad extends OrderedExecutorState {

      protected long p01, p02, p03, p04, p05, p06, p07;
      protected long p10, p11, p12, p13, p14, p15, p16, p17;
   }

   private static final class BoundedOrderedExecutor extends BoundedOrderedExecutorL1Pad implements BoundedExecutor {

      private final MessagePassingQueue<Runnable> commands;
      private final Runnable executeCommandsTask;
      private final Executor delegate;
      private final ThreadLocal<Thread> executorThread;
      private final int maxBurstSize;

      BoundedOrderedExecutor(MessagePassingQueue<Runnable> commands, int maxBurstSize, Executor delegate) {
         this.commands = commands;
         this.executeCommandsTask = this::executeCommands;
         this.delegate = delegate;
         this.maxBurstSize = maxBurstSize;
         this.executorThread = new ThreadLocal<>();
      }

      private boolean tryAcquire() {
         final long oldState = UnsafeAccess.UNSAFE.getAndAddInt(this, STATE_OFFSET, 1);
         final boolean isAcquired = oldState == RELEASED;
         if (isAcquired) {
            executorThread.set(Thread.currentThread());
         }
         return isAcquired;
      }

      private boolean isExecutorThread(Thread thread) {
         final Thread executor = executorThread.get();
         if (executor == null) {
            return false;
         } else {
            return executor == thread;
         }
      }

      private boolean isReleased() {
         return UnsafeAccess.UNSAFE.getIntVolatile(this, STATE_OFFSET) == RELEASED;
      }

      private void release() {
         executorThread.set(null);
         //it is cheaper than the putLongVolatile, on x86 it doesn't add a membar after the store and has anyway release
         //StoreStore + LoadStore
         UnsafeAccess.UNSAFE.storeFence();
         UnsafeAccess.UNSAFE.putInt(this, STATE_OFFSET, RELEASED);
      }

      private void executeCommands() {
         while (!this.commands.isEmpty() && tryAcquire()) {
            try {
               drainCommands(this.commands, this.maxBurstSize);
            } finally {
               release();
            }
         }
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
      public boolean isEmpty() {
         return this.commands.isEmpty();
      }

      @Override
      public int pendingTasks() {
         return this.commands.size();
      }

      @Override
      public boolean tryExecute(Runnable command) {
         final Thread producerThread = Thread.currentThread();
         if (isExecutorThread(producerThread)) {
            command.run();
            return true;
         } else {
            if (commands.relaxedOffer(command)) {
               if (isReleased() && !commands.isEmpty()) {
                  this.delegate.execute(executeCommandsTask);
               }
               return true;
            } else {
               return false;
            }
         }
      }

      @Override
      public void execute(Runnable command) {
         final Thread producerThread = Thread.currentThread();
         if (isExecutorThread(producerThread)) {
            command.run();
         } else {
            while (!commands.offer(command)) {
               if (producerThread.isInterrupted()) {
                  throw new IllegalStateException("can't execute the command; interrupted producer thread!");
               }
               LockSupport.parkNanos(1L);
            }
            if (isReleased() && !commands.isEmpty()) {
               this.delegate.execute(executeCommandsTask);
            }
         }
      }
   }
}
