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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.jctools.queues.MpscChunkedArrayQueue;
import org.jctools.util.Pow2;

public final class AgentProcessor implements AgentManager {

   private final WaitStrategy waitStrategy;
   private final AtomicReference<AgentSubscription<?>> subscription;
   private final int defaultAgentCapacity;
   private final int maxBurstSize;

   private volatile boolean closed;

   public AgentProcessor(WaitStrategy waitStrategy) {
      this.waitStrategy = waitStrategy;
      this.closed = false;
      this.maxBurstSize = 32;
      this.defaultAgentCapacity = 1024;
      this.subscription = new AtomicReference<>();
   }

   @Override
   public void run() {
      long waitTimes = 0;
      final WaitStrategy waitStrategy = this.waitStrategy;
      final AtomicReference<AgentSubscription<?>> subscription = this.subscription;
      final int maxBurstSize = this.maxBurstSize;
      //just in case the conditional await could miss a wakeup :)
      final long maxWaitTime = TimeUnit.SECONDS.toNanos(1);
      AgentSubscription<?> currentSubscription = null;
      //wait a subscription
      while (!this.closed && (currentSubscription = subscription.get()) == null) {
         //something has happened -> continue until there is something left to do (exploit a temporal bet :))
         //nothing has happened -> rely on the wait strategy
         try {
            waitTimes = waitStrategy.waitFor(waitTimes, maxWaitTime);
         } catch (InterruptedException e) {
            this.closed = true;
         }
      }
      waitTimes = 0;
      if (currentSubscription != null) {
         while (!this.closed) {
            if (!currentSubscription.isClosed()) {
               final long operations = currentSubscription.dutyCycle(maxBurstSize);
               System.err.println(operations);
               if (currentSubscription.isClosed()) {
                  this.closed = true;
               } else {
                  if (operations > 0) {
                     waitTimes = 0;
                  } else {
                     try {
                        waitTimes = waitStrategy.waitFor(waitTimes, maxWaitTime);
                     } catch (InterruptedException e) {
                        this.closed = true;
                     }
                  }
               }
            } else {
               this.closed = true;
            }
         }
      }
   }

   @Override
   public <E> AgentSubscription<E> registerExclusive(Consumer<? super E> eventHandler) {
      if (closed)
         throw new IllegalStateException("group already closed");
      //multi producer queue
      final AgentSubscription<E> agentSubscription = new AgentSubscription<>(this.waitStrategy, new MpscChunkedArrayQueue<E>(this.defaultAgentCapacity, Pow2.MAX_POW2), eventHandler);
      if (!subscription.compareAndSet(null, agentSubscription)) {
         throw new IllegalStateException("can't add another subscription");
      }
      waitStrategy.wakeUp();
      return agentSubscription;
   }

   @Override
   public <E> AgentSubscription<E> registerShared(Consumer<? super E> eventHandler) {
      if (closed)
         throw new IllegalStateException("group already closed");
      //multi producer queue
      final AgentSubscription<E> agentSubscription = new AgentSubscription<>(this.waitStrategy, new MpscChunkedArrayQueue<E>(this.defaultAgentCapacity, Pow2.MAX_POW2), eventHandler);
      if (!subscription.compareAndSet(null, agentSubscription)) {
         throw new IllegalStateException("can't add another subscription");
      }
      waitStrategy.wakeUp();
      return agentSubscription;
   }

   @Override
   public void close() {
      //it safe to be called only when all the subscriptions are closed first -> delegate the need to flush them
      if (!this.closed) {
         this.closed = true;
         waitStrategy.wakeUp();
      }
   }

}

