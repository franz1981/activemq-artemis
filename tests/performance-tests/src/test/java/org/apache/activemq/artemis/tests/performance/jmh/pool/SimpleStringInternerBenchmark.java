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

package org.apache.activemq.artemis.tests.performance.jmh.pool;

import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.junit.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@OutputTimeUnit(TimeUnit.SECONDS)
@BenchmarkMode(Mode.Throughput)
@State(Scope.Benchmark)
public class SimpleStringInternerBenchmark {

   private static final String[] UUIDS = {
      "785538b0-f303-11e7-8c3f-9a214cf093ae",
      "78553d06-f303-11e7-8c3f-9a214cf093ae",
      "78553f22-f303-11e7-8c3f-9a214cf093ae",
      "78554120-f303-11e7-8c3f-9a214cf093ae",
      "7855430a-f303-11e7-8c3f-9a214cf093ae",
      "78554512-f303-11e7-8c3f-9a214cf093ae",
      "785549ae-f303-11e7-8c3f-9a214cf093ae",
      "78554ba2-f303-11e7-8c3f-9a214cf093ae"
   };

   @Param({"8", "16", "36"})
   private int length;
   private Interner<SimpleString> guavaInterner;
   private SimpleString.Interner artemisInterner;
   private ByteBuf[] byteBufs;
   //UUIDS.length must be power of 2!
   private static int mask = UUIDS.length - 1;

   @Setup
   public void init() {
      guavaInterner = Interners.newWeakInterner();
      artemisInterner = new SimpleString.Interner(UUIDS.length * 2, length * 2);
      byteBufs = Stream.of(UUIDS).map(uuid -> {
         final SimpleString uuidSimple = new SimpleString(uuid.substring(0, length));
         final int expectedLength = (uuidSimple.length() * 2) + 4;
         final ByteBuf byteBuf = UnpooledByteBufAllocator.DEFAULT.heapBuffer(expectedLength, expectedLength);
         SimpleString.writeSimpleString(byteBuf, uuidSimple);
         return byteBuf;
      }).collect(Collectors.toList()).toArray(new ByteBuf[UUIDS.length]);
   }

   @State(Scope.Thread)
   public static class Sequence {

      ByteBuf[] byteBufs;
      long sequence;

      @Setup(Level.Trial)
      public void initByteBufs(SimpleStringInternerBenchmark benchmark) {
         byteBufs = Stream.of(benchmark.byteBufs).map(ByteBuf::copy).collect(Collectors.toList()).toArray(new ByteBuf[UUIDS.length]);
      }

      @Setup(Level.Iteration)
      public void reset() {
         sequence = 0;
      }

      public ByteBuf nextByteBuf() {
         final int index = (int) (sequence & mask);
         sequence++;
         return byteBufs[index].resetReaderIndex();
      }
   }

   @Benchmark
   public SimpleString artemisIntern(Sequence sequence) {
      final ByteBuf byteBuf = sequence.nextByteBuf();
      final int length = byteBuf.readInt();
      return artemisInterner.intern(byteBuf, length);
   }

   @Benchmark
   @GroupThreads(3)
   public SimpleString artemisIntern3Threads(Sequence sequence) {
      final ByteBuf byteBuf = sequence.nextByteBuf();
      final int length = byteBuf.readInt();
      return artemisInterner.intern(byteBuf, length);
   }

   @Benchmark
   public SimpleString guavaInterner(Sequence sequence) {
      final ByteBuf byteBuf = sequence.nextByteBuf();
      return guavaInterner.intern(SimpleString.readSimpleString(byteBuf));
   }

   @Benchmark
   @GroupThreads(3)
   public SimpleString guavaInterner3Threads(Sequence sequence) {
      final ByteBuf byteBuf = sequence.nextByteBuf();
      return guavaInterner.intern(SimpleString.readSimpleString(byteBuf));
   }

   @Benchmark
   public SimpleString noIntern(Sequence sequence) {
      final ByteBuf byteBuf = sequence.nextByteBuf();
      return SimpleString.readSimpleString(byteBuf);
   }

   @Benchmark
   @GroupThreads(3)
   public SimpleString noIntern3Threads(Sequence sequence) {
      final ByteBuf byteBuf = sequence.nextByteBuf();
      return SimpleString.readSimpleString(byteBuf);
   }

   @Test
   public void run() throws RunnerException {
      final Options opt = new OptionsBuilder()
         .include(SimpleStringInternerBenchmark.class.getSimpleName())
         .addProfiler(GCProfiler.class)
         .jvmArgs("-XX:+UseG1GC")
         .warmupIterations(5)
         .measurementIterations(5)
         .forks(2)
         .build();
      new Runner(opt).run();
   }
}
