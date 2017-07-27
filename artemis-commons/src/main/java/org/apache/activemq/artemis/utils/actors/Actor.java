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

package org.apache.activemq.artemis.utils.actors;

import java.util.concurrent.Executor;
import java.util.function.Consumer;

public final class Actor<T> extends ProcessorBase<T> {

   private final Consumer<? super T> listener;

   public Actor(Executor parent, Consumer<? super T> listener) {
      super(parent);
      this.listener = listener;
   }

   @Override
   protected void doTask(T task) {
      listener.accept(task);
   }

   public void act(T message) {
      task(message);
   }

}
