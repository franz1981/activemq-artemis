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

package org.apache.activemq.artemis.tests.extras.benchmarks.pools;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@State(Scope.Group)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Warmup(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
public class ThreadPoolBenchmark {

   private static final ThreadLocal<Object> MARKER = new ThreadLocal<Object>();
   @Param({"baseline", "old", "new"})
   private String type;
   private ExecutorService poolExecutor;
   private Runnable task;
   private AtomicReferenceArray<CounterThread> counters;
   private long submitted;

   //IMPORTANT: must be run with -XX:-UseBiasedLocking !!!
   public static void main(String[] args) throws Exception {
      final Options opt = new OptionsBuilder().addProfiler(GCProfiler.class).shouldDoGC(true).include(ThreadPoolBenchmark.class.getSimpleName()).build();
      new Runner(opt).run();
   }

   @Setup(Level.Trial)
   public void init() {
      this.counters = new AtomicReferenceArray<CounterThread>(3);

      final AtomicInteger counterIndex = new AtomicInteger(0);
      final ThreadFactory threadFactory = r -> {
         final int index = counterIndex.getAndIncrement();
         final CounterThread thread = new CounterThread(index, r);
         this.counters.lazySet(index, thread);
         return thread;
      };
      switch (type) {
         case "old":
            poolExecutor = new org.apache.activemq.artemis.utils.ActiveMQThreadPoolExecutor(3, 3, 10, TimeUnit.DAYS, threadFactory);
            break;
         case "new":
            poolExecutor = new ActiveMQThreadPoolExecutor(3, 3, 10, TimeUnit.DAYS, threadFactory);
            break;
         case "baseline":
            poolExecutor = Executors.newFixedThreadPool(3, threadFactory);
            break;
      }
      this.task = () -> {
         //update the current thread value
         final CounterThread counterThread = (CounterThread) Thread.currentThread();
         final long value = counterThread.value;
         final long nextValue = value + 1L;
         counterThread.value = nextValue;
         //LoadWrite + WriteWrite -> store release nextValue
         //publish the value in order to be read by other threads
         counterThread.countView.lazySet(nextValue);
      };
      //wait started executor
      try {
         poolExecutor.submit(() -> {
         }).get();
      } catch (InterruptedException e) {
         e.printStackTrace();
      } catch (ExecutionException e) {
         e.printStackTrace();
      }
   }

   @Benchmark
   @Group("tpt")
   public void execute(ProducerMarker producerMarker) {
      //the MARKER is used only to perform cleanup of the state
      this.submitted++;
      poolExecutor.execute(this.task);
   }

   @Benchmark
   @Group("tpt")
   public void pollTaskStatus(ExecutorCounters counters) {
      //update the status of the task executed
      CounterThread thread;
      thread = this.counters.get(0);
      counters.thread_1 = thread == null ? 0 : thread.countView.get();
      thread = this.counters.get(1);
      counters.thread_2 = thread == null ? 0 : thread.countView.get();
      thread = this.counters.get(2);
      counters.thread_3 = thread == null ? 0 : thread.countView.get();
   }

   @TearDown(Level.Iteration)
   public void waitExecutedTasks() {
      //executed by each thread of the benchmark group BUT
      //only the producer thread could perform the cleanup :)
      final boolean isProducer = MARKER.get() != null;
      if (!isProducer) {
         return;
      }
      try {
         while (true) {
            long messages = 0;
            for (int i = 0; i < 3; i++) {
               final CounterThread thread = this.counters.get(i);
               messages += thread == null ? 0L : thread.countView.get();
            }
            if (messages == this.submitted) {
               break;
            }
         }
      } finally {
         //we are sure the executor services has executed all the tasks!
         for (int i = 0; i < 3; i++) {
            final CounterThread thread = this.counters.get(i);
            if (thread != null) {
               thread.value = 0;
               thread.countView.lazySet(0L);
            }
         }
         //reset submitted
         this.submitted = 0;
      }
   }

   //will be called by anyone
   @TearDown(Level.Trial)
   public void destroy() {
      try {
         poolExecutor.shutdown();
         poolExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
      } catch (InterruptedException e) {
         e.printStackTrace();
      }
   }

   @State(Scope.Thread)
   public static class ProducerMarker {

      public ProducerMarker() {
         MARKER.set(this);
      }
   }

   @AuxCounters
   @State(Scope.Thread)
   public static class ExecutorCounters {

      public long thread_1;
      public long thread_2;
      public long thread_3;

      @Setup(Level.Iteration)
      public void clean() {
         thread_1 = 0;
         thread_2 = 0;
         thread_3 = 0;
      }
   }

   private static final class CounterThread extends Thread {

      private final int index;
      private final AtomicLong countView = new AtomicLong();
      private long value = 0;

      private CounterThread(int index, Runnable task) {
         super(task);
         this.index = index;
      }

   }
}
