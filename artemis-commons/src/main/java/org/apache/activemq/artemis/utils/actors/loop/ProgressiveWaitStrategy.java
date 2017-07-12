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

import java.util.concurrent.locks.LockSupport;

public enum ProgressiveWaitStrategy implements WaitStrategy {
   Instance;

   @Override
   public long waitFor(long waitTimes, long nanoTimeToWait) throws InterruptedException {
      if (waitTimes < 100) {
         waitTimes++;
      } else if (waitTimes < 1000) {
         Thread.yield();
         waitTimes++;
      } else {
         //ignore the nanoTimeToWait that is only an hint
         LockSupport.parkNanos(1L);
      }
      return waitTimes;
   }

   @Override
   public void wakeUp() {

   }
}
