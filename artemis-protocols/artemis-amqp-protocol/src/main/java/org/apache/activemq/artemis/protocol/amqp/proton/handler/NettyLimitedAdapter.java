/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.protocol.amqp.proton.handler;


import io.netty.channel.EventLoop;
import org.apache.activemq.artemis.utils.actors.OrderedExecutor;

/** This executor will use the Netty Executor in a limited way.
 *  It will limit the number of runnables and will let Netty to perform other work.
 *
 *  This is an attempt to save Executor from waking up a Netty epoll or any other native thread.
 *  With this we kind of batch executors into Netty worker. */
public class NettyLimitedAdapter extends OrderedExecutor {

   private final EventLoop eventLoop;
   private final int limit;
   private int count;

   public NettyLimitedAdapter(EventLoop eventLoop, int limit) {
      super(eventLoop);
      this.limit = limit;
      this.eventLoop = eventLoop;
   }

   @Override
   protected void enter() {
      super.enter();
      count = 0;
   }

   @Override
   public boolean inHandler(Thread thread) {
      return eventLoop.inEventLoop(thread);
   }

   @Override
   protected boolean doTask(Runnable task) {
      super.doTask(task);

      // don't worry about locks, this is single threaded

      if (++count >= limit) {
         count = 0;
         runDelegate();
         return false;
      } else {
         return true;
      }

   }
}
