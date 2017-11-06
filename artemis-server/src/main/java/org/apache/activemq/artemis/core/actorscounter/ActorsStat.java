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

package org.apache.activemq.artemis.core.actorscounter;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.apache.activemq.artemis.utils.actors.ProcessorBase;

public enum ActorsStat {
   Instance;

   private final Thread timer;

   ActorsStat() {
      timer = new Thread(() -> {
         final long delay = TimeUnit.SECONDS.toNanos(1);
         while (!Thread.currentThread().isInterrupted()) {
            doWork();
            LockSupport.parkNanos(delay);
         }
      });
      timer.setName("actors-stats");
      timer.setDaemon(true);
      timer.start();
      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
         timer.interrupt();
      }));
   }

   private static final class ActorStat {

      private final WeakReference<? extends ProcessorBase<?>> actorRef;
      private final String name;
      private long lastSampleTime;
      private long lastErrors = 0;
      private long lastConsumed = 0;
      private long lastSubmitted = 0;

      ActorStat(ProcessorBase<?> actor, String name) {
         this.lastSampleTime = System.currentTimeMillis();
         this.lastErrors = actor.errors();
         this.lastConsumed = actor.consumed();
         this.lastSubmitted = actor.submitted();
         this.actorRef = new WeakReference<ProcessorBase<?>>(actor);
         this.name = name;
      }
   }

   private interface ActorsStatCommand {

      void executeOn(ActorsStat stat);
   }

   private static final class AddActor implements ActorsStatCommand {

      private final WeakReference<? extends ProcessorBase<?>> actorRef;
      private final String name;

      AddActor(ProcessorBase<?> actor, String name) {
         this.actorRef = new WeakReference<ProcessorBase<?>>(actor);
         this.name = name;
      }

      @Override
      public void executeOn(ActorsStat stat) {
         final ProcessorBase<?> actor = actorRef.get();
         if (actor != null) {
            stat.onRegister(actor, name);
         }
      }
   }

   private static final class RemoveActor implements ActorsStatCommand {

      private final ProcessorBase<?> actor;

      RemoveActor(ProcessorBase<?> actor) {
         this.actor = actor;
      }

      @Override
      public void executeOn(ActorsStat stat) {
         stat.onDeregister(actor);
      }
   }

   private final ConcurrentLinkedQueue<ActorsStatCommand> commands = new ConcurrentLinkedQueue<>();
   private final List<ActorStat> actors = new ArrayList<>();

   public void register(ProcessorBase<?> actor, String name) {
      this.commands.offer(new AddActor(actor, name));
   }

   public void deregister(final ProcessorBase<?> actorToRemove) {
      this.commands.offer(new RemoveActor(actorToRemove));
   }

   private void onDeregister(ProcessorBase<?> actor) {
      actors.remove(actor);
   }

   private void onRegister(ProcessorBase<?> actor, String name) {
      actors.add(new ActorStat(actor, name));
   }

   private long doWork() {
      long worksDone = 0;
      ActorsStatCommand command;
      while ((command = this.commands.poll()) != null) {
         command.executeOn(this);
         worksDone++;
      }
      worksDone += gc();
      worksDone += sample();
      return worksDone;
   }

   private int sample() {
      int live = 0;
      System.out.print("\033[H\033[2J");
      for (int i = 0, size = actors.size(); i < size; i++) {
         final ActorStat stat = actors.get(i);
         final ProcessorBase<?> actor = stat.actorRef.get();
         if (actor != null) {
            final long now = System.currentTimeMillis();
            final long submittedNow = actor.submitted();
            final long consumedNow = actor.consumed();
            final long errorsNow = actor.errors();
            final long elapsed = now - stat.lastSampleTime;
            final long submitted = submittedNow - stat.lastSubmitted;
            final long consumed = consumedNow - stat.lastConsumed;
            final long errors = errorsNow - stat.lastErrors;
            System.out.format("[%s]\tDuration %d ms - In %,d msg - Out %,d msg - Errors %,d %n", stat.name, elapsed, submitted, consumed, errors);
            stat.lastSampleTime = now;
            stat.lastConsumed = consumedNow;
            stat.lastSubmitted = submittedNow;
            stat.lastErrors = errorsNow;
            live++;
         }
      }
      return live;
   }

   private int gc() {
      int removed = 0;
      //garbage free fast remove using ArrayLists that maintain the original order
      for (int i = actors.size() - 1; i >= 0; i--) {
         final ActorStat actorStat = actors.get(i);
         if (actorStat.actorRef.get() == null) {
            actors.remove(i);
            removed++;
         }
      }
      return removed;
   }

}
