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
package org.apache.activemq.artemis.cli.commands.messages;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BooleanSupplier;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogWriter;
import org.HdrHistogram.Recorder;
import org.HdrHistogram.SingleWriterRecorder;
import org.HdrHistogram.ValueRecorder;

public class StatsUtil {

   public static void printAndCollectStats(String statsFileName,
                                           LongAdder counter,
                                           ValueRecorder recorder,
                                           long collectIntervalMs,
                                           BooleanSupplier exitCondition) throws FileNotFoundException {
      long oldTime = System.nanoTime();
      long oldCount = counter.sum();
      Histogram reportHistogram = null;
      HistogramLogWriter histogramLogWriter = null;
      try {
         if (statsFileName != null) {
            System.out.println("Dumping latency stats to " + statsFileName);
            PrintStream histogramLog = new PrintStream(new FileOutputStream(statsFileName), false);
            histogramLogWriter = new HistogramLogWriter(histogramLog);
            histogramLogWriter.outputLogFormatVersion();
            histogramLogWriter.outputLegend();
         }
         while (!exitCondition.getAsBoolean()) {
            TimeUnit.MILLISECONDS.sleep(collectIntervalMs);
            long now = System.nanoTime();
            double elapsed = (now - oldTime) / 1_000_000_000L;
            long count = counter.sum();
            double rate = (count - oldCount) / elapsed;

            if (histogramLogWriter != null) {
               if (recorder instanceof Recorder) {
                  reportHistogram = ((Recorder) recorder).getIntervalHistogram(reportHistogram);
               } else if (recorder instanceof SingleWriterRecorder) {
                  reportHistogram = ((SingleWriterRecorder) recorder).getIntervalHistogram(reportHistogram);
               }
            }
            System.out.println("Throughput:\t " + rate + "\tmsg/s");
            if (histogramLogWriter != null) {
               histogramLogWriter.outputIntervalHistogram(reportHistogram);
            }

            oldTime = now;
            oldCount = count;
         }
      } catch (InterruptedException ie) {
         if (histogramLogWriter != null) {
            if (recorder instanceof Recorder) {
               reportHistogram = ((Recorder) recorder).getIntervalHistogram(reportHistogram);
            } else if (recorder instanceof SingleWriterRecorder) {
               reportHistogram = ((SingleWriterRecorder) recorder).getIntervalHistogram(reportHistogram);
            }
         }
         if (histogramLogWriter != null) {
            histogramLogWriter.outputIntervalHistogram(reportHistogram);
            reportHistogram.reset();
         }
      } finally {
         if (histogramLogWriter != null) {
            histogramLogWriter.close();
         }
      }
   }
}
