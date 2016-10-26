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

package org.apache.activemq.transport.amqp.client;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.junit.Test;

public class AmqpFrameValidatorVisibilityTest {

   @Test
   public void shouldBeReadAsInvalid() {
      final AmqpFrameValidator amqpFrameValidator = new AmqpFrameValidator();
      final CountDownLatch finished = new CountDownLatch(1);
      final Thread invalidReader = new Thread(new Runnable() {
         @Override
         public void run() {
            //when AmqpFrameValidator::isValid will be JIT compiled, it's return value will be hoisted out! -> CONSTANT!
            boolean isValid = amqpFrameValidator.isValid();
            while(isValid){
               isValid = amqpFrameValidator.isValid();
            }
            finished.countDown();
         }

      });
      invalidReader.setDaemon(true);
      invalidReader.start();
      //added to let the JVM OSR the reader loop
      LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(10));
      amqpFrameValidator.markAsInvalid("DUMMY ERROR MESSAGE");
      //waits 1 minutes to let the reader perceive the value change
      boolean succeed;
      try {
         succeed = finished.await(1, TimeUnit.MINUTES);
      }
      catch (InterruptedException e) {
         succeed = false;
      }
      if (!succeed) {
         //force stop the reader thread
         invalidReader.interrupt();
         try {
            invalidReader.join();
         }
         catch (InterruptedException ie) {
            //NO_OP
         }
         throw new IllegalStateException("the validator hasn't notified it is invalid to the reader!");
      }
   }

}
