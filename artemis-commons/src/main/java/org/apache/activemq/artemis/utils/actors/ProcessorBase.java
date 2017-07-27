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

package org.apache.activemq.artemis.utils.actors;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.jctools.queues.MpscChunkedArrayQueue;
import org.jctools.util.Pow2;

abstract class ProcessorBase<T> {

   private static final int ACTOR_MIN_CAPACITY = Integer.getInteger("processor.min.capacity", 1024);

   private static final int STATE_NOT_RUNNING = 0;
   private static final int STATE_RUNNING = 1;

   protected final MpscChunkedArrayQueue<T> tasks = new MpscChunkedArrayQueue<>(ACTOR_MIN_CAPACITY, Pow2.MAX_POW2);

   private final Executor delegate;

   private final ExecutorTask task = new ExecutorTask();

   // used by stateUpdater
   @SuppressWarnings("unused")
   private volatile int state = 0;

   private static final AtomicIntegerFieldUpdater<ProcessorBase> stateUpdater = AtomicIntegerFieldUpdater.newUpdater(ProcessorBase.class, "state");

   private final class ExecutorTask implements Runnable {

      @Override
      public void run() {
         //we loop again based on tasks not being empty. Otherwise there is a window where the state is running,
         //but poll() has returned null, so a submitting thread will believe that it does not need re-execute.
         //this check fixes the issue
         while (!tasks.isEmpty()) {
            //if there is no thread active then we run
            if (stateUpdater.compareAndSet(ProcessorBase.this, STATE_NOT_RUNNING, STATE_RUNNING)) {
               T task;
               //while the queue is not empty we process in order
               while ((task = tasks.poll()) != null) {
                  doTask(task);
               }
               //set state back to not running.
               stateUpdater.set(ProcessorBase.this, STATE_NOT_RUNNING);
            } else {
               return;
            }
         }
      }
   }

   protected abstract void doTask(T task);

   ProcessorBase(Executor parent) {
      this.delegate = parent;
   }

   public final boolean flush() {
      return flush(30, TimeUnit.SECONDS);
   }

   /**
    * WARNING: This will only flush when all the activity is suspended.
    *          don't expect success on this call if another thread keeps feeding the queue
    *          this is only valid on situations where you are not feeding the queue,
    *          like in shutdown and failover situations.
    * */
   public final boolean flush(long timeout, TimeUnit unit) {
      if (stateUpdater.get(this) == STATE_NOT_RUNNING) {
         // quick test, most of the time it will be empty anyways
         return true;
      }

      long timeLimit = System.currentTimeMillis() + unit.toMillis(timeout);
      try {
         while (stateUpdater.get(this) == STATE_RUNNING && timeLimit > System.currentTimeMillis()) {
            Thread.sleep(10);
         }
      } catch (InterruptedException e) {
         // ignored
      }

      return stateUpdater.get(this) == STATE_NOT_RUNNING;
   }

   public final int pendingTasks() {
      return this.tasks.size();
   }

   public final int capacity() {
      return this.tasks.capacity();
   }

   protected final void task(T command) {
      tasks.add(command);
      startPoller();
   }

   private void startPoller() {
      if (stateUpdater.get(this) == STATE_NOT_RUNNING) {
         //note that this can result in multiple tasks being queued
         //this is not an issue as the CAS will mean that the second (and subsequent) execution is ignored
         delegate.execute(task);
      }
   }

}
