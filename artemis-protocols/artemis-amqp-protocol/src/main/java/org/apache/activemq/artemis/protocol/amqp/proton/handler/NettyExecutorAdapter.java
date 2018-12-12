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

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import io.netty.channel.EventLoop;
import org.apache.activemq.artemis.utils.actors.ArtemisExecutor;

public class NettyExecutorAdapter implements ArtemisExecutor {

   private final EventLoop eventLoop;

   public NettyExecutorAdapter(EventLoop eventLoop) {
      this.eventLoop = eventLoop;
   }

   @Override
   public boolean inHandler(Thread thread) {
      return eventLoop.inEventLoop(thread);
   }

   @Override
   public int shutdownNow(Consumer<? super Runnable> onPendingTask) {
      throw new IllegalStateException("not supported");
   }

   @Override
   public boolean flush(long timeout, TimeUnit unit) {
      throw new IllegalStateException("not supported");
   }

   @Override
   public int shutdownNow() {
      throw new IllegalStateException("not supported");
   }

   @Override
   public void shutdown() {
      throw new IllegalStateException("not supported");
   }

   @Override
   public void execute(Runnable command) {
      eventLoop.execute(command);
   }
}
