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

import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public final class AgentSubscription<E> implements AutoCloseable {

   private static final Object POISON_PILL = new Object();
   private final Consumer<? super E> eventHandler;
   private final Queue<Object> events;
   private final WaitStrategy waitStrategy;
   private final CountDownLatch flushedAndClosed;
   private boolean closed;
   private final AtomicLong processedEvents;
   private boolean poisonPillTaken;

   public AgentSubscription(WaitStrategy waitStrategy, Queue<E> events, Consumer<? super E> eventHandler) {
      this.eventHandler = eventHandler;
      this.events = (Queue<Object>) events;
      this.waitStrategy = waitStrategy;
      this.closed = false;
      this.flushedAndClosed = new CountDownLatch(1);
      //replace it using an IPC counter to be mapped to JMX
      this.processedEvents = new AtomicLong(0);
      this.poisonPillTaken = false;
   }

   public boolean trySubmit(E event) {
      if (closed) {
         throw new IllegalStateException("subscription already closed!");
      }
      if (this.events.offer(event)) {
         this.waitStrategy.wakeUp();
         return true;
      } else {
         return false;
      }
   }

   public long processedEvents() {
      return this.processedEvents.get();
   }

   public int pendingEvents() {
      return this.events.size();
   }

   private static int drainEvents(Queue<?> events, int burstSize) {
      int tasks = 0;
      while (tasks < burstSize && events.poll() != null) {
         tasks++;
      }
      return tasks;
   }

   int dutyCycle(int burstSize) {
      final Queue<E> events = (Queue<E>) this.events;
      final Consumer<? super E> eventHandler = this.eventHandler;
      final AtomicLong processedEvents = this.processedEvents;
      final boolean poisonPillTaken = this.poisonPillTaken;
      int tasks;
      if (poisonPillTaken) {
         tasks = drainEvents(events, burstSize);
         if (poisonPillTaken && tasks > 0) {
            System.err.println("taken " + tasks + " after the poison pill!");
         }
      } else {
         E event;
         tasks = 0;
         while (tasks < burstSize && (event = events.poll()) != null) {
            tasks++;
            if (event == POISON_PILL) {
               this.poisonPillTaken = true;
               this.flushedAndClosed.countDown();
               return tasks;
            } else {
               try {
                  eventHandler.accept(event);
                  processedEvents.lazySet(processedEvents.get() + 1);
               } catch (Throwable t) {
                  //TODO collect/log errors
                  System.err.println(t);
               }
            }
         }
      }
      return tasks;
   }

   public boolean isClosed() {
      return this.closed;
   }

   @Override
   public void close() {
      //it is safe to be called only by on thread!!
      if (!this.closed) {
         if (!this.events.offer(POISON_PILL)) {
            throw new IllegalStateException("can't close the subscription");
         }
         //wakeup to let the pill get digested
         final long deadLineWhileWaiting = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
         boolean digested = false;
         try {
            while (!digested && System.nanoTime() < deadLineWhileWaiting) {
               this.waitStrategy.wakeUp();
               try {
                  digested = this.flushedAndClosed.await(1, TimeUnit.SECONDS);
               } catch (InterruptedException e) {
                  //TODO assume digested but log the error!
                  System.err.println(e);
                  digested = true;
               }
            }
         } finally {
            this.closed = true;
            //wake up to let the group dispose a tomb for it
            this.waitStrategy.wakeUp();
         }
      }
   }

   public void flush() {
      this.waitStrategy.wakeUp();
   }
}
