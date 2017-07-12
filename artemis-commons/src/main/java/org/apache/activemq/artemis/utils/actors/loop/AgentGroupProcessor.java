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
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.jctools.queues.MpscChunkedArrayQueue;
import org.jctools.queues.SpscChunkedArrayQueue;
import org.jctools.util.Pow2;

public final class AgentGroupProcessor implements AgentManager {

   private final WaitStrategy waitStrategy;
   private final List<AgentSubscription<?>> subscriptions;
   private final Queue<AgentSubscription<?>> subscriptionsToAdd;
   private final int maxBurstSize;
   private final int defaultAgentCapacity;

   private volatile boolean closed;

   public AgentGroupProcessor(WaitStrategy waitStrategy) {
      this.subscriptions = new ArrayList<>();
      this.waitStrategy = waitStrategy;
      this.closed = false;
      //avoid too big burst size to make the agent schedule fairest
      this.maxBurstSize = 32;
      this.defaultAgentCapacity = 1024;
      this.subscriptionsToAdd = new MpscChunkedArrayQueue<>(32);
   }

   private static int addSubscriptions(Queue<AgentSubscription<?>> subscriptionsToAdd,
                                       List<AgentSubscription<?>> subscriptions) {
      int added = 0;
      AgentSubscription<?> subscription;
      while ((subscription = subscriptionsToAdd.poll()) != null) {
         subscriptions.add(subscription);
         added++;
      }
      return added;
   }

   private static int removeSubscriptions(List<AgentSubscription<?>> subscriptions) {
      int deads = 0;
      final int size = subscriptions.size();
      //reverse order to make the array copy cheaper and safe to be performed
      for (int i = size - 1; i >= 0; i--) {
         final AgentSubscription<?> subscription = subscriptions.get(i);
         if (subscription == null || subscription.isClosed()) {
            subscriptions.remove(i);
            deads++;
         }
      }
      return deads;
   }

   @Override
   public void run() {
      long waitTimes = 0;
      final WaitStrategy waitStrategy = this.waitStrategy;
      final List<AgentSubscription<?>> subscriptions = this.subscriptions;
      final Queue<AgentSubscription<?>> subscriptionsToAdd = this.subscriptionsToAdd;
      final int maxBurstSize = this.maxBurstSize;
      //just in case the conditional await could miss a wakeup :)
      final long maxWaitTime = TimeUnit.SECONDS.toNanos(1);

      while (!this.closed) {
         long operations = 0;
         //add what need to be added
         if (!subscriptionsToAdd.isEmpty()) {
            operations += addSubscriptions(subscriptionsToAdd, subscriptions);
         }
         //perform the dutyCycles on them
         final int subscriptionsSize = subscriptions.size();
         int tombstones = 0;
         for (int i = 0; i < subscriptionsSize; i++) {
            final AgentSubscription<?> subscription = subscriptions.get(i);
            if (!subscription.isClosed()) {
               operations += subscription.dutyCycle(maxBurstSize);
            }
            if (subscription.isClosed()) {
               //put a tombstone to let the next pass deal with the dead
               subscriptions.set(i, null);
               tombstones++;
            }
         }
         if (tombstones > 0) {
            operations += removeSubscriptions(subscriptions);
         }
         //something has happened -> continue until there is something left to do (exploit a temporal bet :))
         //nothing has happened -> rely on the wait strategy
         if (operations == 0) {
            try {
               waitTimes = waitStrategy.waitFor(waitTimes, maxWaitTime);
            } catch (InterruptedException e) {
               this.closed = true;
            }
         } else {
            waitTimes = 0;
         }
      }
      //no need to flush: by contract the agent group can't be closed if any subscriptions are alive
   }

   @Override
   public <E> AgentSubscription<E> registerExclusive(Consumer<? super E> eventHandler) {
      if (closed)
         throw new IllegalStateException("group already closed");
      //single producer queue
      final AgentSubscription<E> agentSubscription = new AgentSubscription<>(this.waitStrategy, new SpscChunkedArrayQueue<E>(this.defaultAgentCapacity, Pow2.MAX_POW2), eventHandler);
      subscriptionsToAdd.add(agentSubscription);
      waitStrategy.wakeUp();
      return agentSubscription;
   }

   @Override
   public <E> AgentSubscription<E> registerShared(Consumer<? super E> eventHandler) {
      if (closed)
         throw new IllegalStateException("group already closed");
      //multi producer queue
      final AgentSubscription<E> agentSubscription = new AgentSubscription<>(this.waitStrategy, new MpscChunkedArrayQueue<E>(this.defaultAgentCapacity, Pow2.MAX_POW2), eventHandler);
      subscriptionsToAdd.add(agentSubscription);
      waitStrategy.wakeUp();
      return agentSubscription;
   }

   @Override
   public void close() {
      //it safe to be called only when all the subscriptions are closed first -> delegate the need to flush them
      if (!closed) {
         this.closed = true;
         waitStrategy.wakeUp();
      }
   }

}
