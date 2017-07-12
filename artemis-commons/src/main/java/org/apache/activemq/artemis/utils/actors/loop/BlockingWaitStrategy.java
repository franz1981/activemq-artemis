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

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public final class BlockingWaitStrategy implements WaitStrategy {

   private final Lock lock = new ReentrantLock();
   private final Condition eventEnqueued = lock.newCondition();

   @SuppressWarnings("WaitNotInLoop")
   @Override
   public long waitFor(long waitTimes, long nanoTimeToWait) throws InterruptedException {
      lock.lock();
      try {
         eventEnqueued.awaitNanos(nanoTimeToWait);
      } finally {
         lock.unlock();
      }
      return waitTimes;
   }

   @Override
   public void wakeUp() {
      lock.lock();
      try {
         eventEnqueued.signalAll();
      } finally {
         lock.unlock();
      }
   }
}
