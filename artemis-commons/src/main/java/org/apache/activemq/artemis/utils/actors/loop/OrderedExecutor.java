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

import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

public final class OrderedExecutor implements Executor, AutoCloseable {

   private final AgentSubscription<? super Runnable> agentSubscription;

   private OrderedExecutor(AgentSubscription<? super Runnable> agentSubscription) {
      this.agentSubscription = agentSubscription;
   }

   @Override
   public void execute(Runnable command) {
      if (!agentSubscription.trySubmit(command)) {
         throw new RejectedExecutionException("can't submit the command!");
      }
   }

   @Override
   public void close() throws Exception {
      //will close and flush
      this.agentSubscription.close();
   }

   public static OrderedExecutor exclusive(AgentGroupProcessor agentGroupProcessor) {
      return new OrderedExecutor(agentGroupProcessor.registerExclusive(Runnable::run));
   }

   public static OrderedExecutor shared(AgentGroupProcessor agentGroupProcessor) {
      return new OrderedExecutor(agentGroupProcessor.registerShared(Runnable::run));
   }
}
