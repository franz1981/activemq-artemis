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

package org.apache.activemq.artemis.core.io.buffer;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * This IO limiter is to be considered best effort due to the small compensation that provides (<=100 ms).
 * The algorithm avoid that n consecutive IOs performed in less of 100ms could exceed the expected IOPS limit.
 */
final class BestEffortIOLimiter implements IOLimiter {

   //after IO_INTERVAL_NANOS each previous IOs will not be considered anymore -> no compensation is required
   private static final long IO_INTERVAL_NANOS = TimeUnit.MILLISECONDS.toNanos(100);
   private long startOfInterval;
   private long ioPerInterval;
   private final long maxIOperInterval;

   public BestEffortIOLimiter(int iops) {
      //turn iops in io per interval
      this.startOfInterval = System.nanoTime() - (IO_INTERVAL_NANOS + 1);
      this.ioPerInterval = 0L;
      //turn the iops to be interval based
      this.maxIOperInterval = iops / (TimeUnit.SECONDS.toNanos(1) / IO_INTERVAL_NANOS);
   }

   @Override
   public long limit(final int lastIO) {
      final long now = System.nanoTime();
      final long lastIoTime = this.startOfInterval;
      //do not compensate IO if has passed at least IO_INTERVAL_NANOS from the latest flush
      final long timeBetweenIOs = now - lastIoTime;
      if (timeBetweenIOs > IO_INTERVAL_NANOS) {
         //record this IO as the last one == the start of a new interval
         this.startOfInterval = now;
         this.ioPerInterval = lastIO;
         return 0;
      }
      final long totalIoCurrentPeriod = this.ioPerInterval + lastIO;
      //check if IO need compensation
      if (totalIoCurrentPeriod >= this.maxIOperInterval) {
         return compensateOnIoOverflow(now, timeBetweenIOs);
      } else {
         //accumulate IO from the beginning of the current the interval -> do not change startOfInterval
         this.ioPerInterval = totalIoCurrentPeriod;
         return 0;
      }
   }

   private long compensateOnIoOverflow(final long now, final long timeBetweenIOs) {
      final long timeRemainingInPeriod = IO_INTERVAL_NANOS - timeBetweenIOs;
      LockSupport.parkNanos(timeRemainingInPeriod);
      final long endCompensation = System.nanoTime();
      //believe that the compensation has taken place even on interruption -> ioPerInterval = 0
      this.startOfInterval = endCompensation;
      this.ioPerInterval = 0;
      //real compensation
      final long compensationTime = endCompensation - now;
      return compensationTime;
   }
}
