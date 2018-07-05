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

package org.apache.activemq.artemis.core.io.aio;

import com.lmax.disruptor.AlertException;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.WaitStrategy;
import org.apache.activemq.artemis.jlibaio.LibaioContext;

final class AioPollWaitStrategy implements WaitStrategy {

   private static final int SPIN_TRIES = 100;
   private final LibaioContext<AIOSequentialFileFactory.AIOSequentialCallback> libaioContext;

   AioPollWaitStrategy(LibaioContext<AIOSequentialFileFactory.AIOSequentialCallback> libaioContext) {
      this.libaioContext = libaioContext;
   }

   @Override
   public long waitFor(final long sequence,
                       Sequence cursor,
                       final Sequence dependentSequence,
                       final SequenceBarrier barrier) throws AlertException, InterruptedException {
      long availableSequence;
      int counter = SPIN_TRIES;
      final int queueSize = libaioContext.queueSize;

      while (libaioContext.poll(queueSize) > 0) {

      }
      while ((availableSequence = dependentSequence.get()) < sequence) {
         while (libaioContext.poll(queueSize) > 0) {

         }
         counter = applyWaitMethod(barrier, counter);
      }

      return availableSequence;
   }

   @Override
   public void signalAllWhenBlocking() {
   }

   private int applyWaitMethod(final SequenceBarrier barrier, int counter) throws AlertException {
      barrier.checkAlert();

      if (0 == counter) {
         Thread.yield();
      } else {
         --counter;
      }

      return counter;
   }
}

