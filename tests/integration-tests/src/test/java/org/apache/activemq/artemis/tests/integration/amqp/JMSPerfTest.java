/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.tests.integration.amqp;

import java.io.PrintStream;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.SingleWriterRecorder;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSConstants;
import org.apache.activemq.artemis.core.remoting.impl.invm.TransportConstants;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.junit.Test;

import static java.lang.String.format;

public class JMSPerfTest extends JMSClientTestSupport {

   @Override
   protected void addConfiguration(ActiveMQServer server) {
      server.getConfiguration().setAmqpUseCoreSubscriptionNaming(true);
      server.getConfiguration().setPersistenceEnabled(false);
      HashMap<String, Object> params = new HashMap<>();
      params.put(TransportConstants.DIRECT_DELIVER, true);
      server.getConfiguration().getAcceptorConfigurations().add(new TransportConfiguration(INVM_ACCEPTOR_FACTORY, params));
   }

   protected Connection createInVMConnection() throws JMSException {
      return createCoreConnection("vm://0", null, null, null, true);
   }

   @Override
   protected String getConfiguredProtocols() {
      return "AMQP,OPENWIRE,CORE";
   }

   private void testSharedDurableConsumer(Connection connection1, Connection connection2) throws JMSException, InterruptedException {
      try {
         for (int i = 0; i < 1; i++)
            testSharedDurableConsumer2(connection1, connection2);
      } finally {
         connection1.close();
         connection2.close();
      }
   }

   public enum OutputFormat {
      LONG {
         @Override
         public void output(final Histogram histogram, PrintStream out, double outputScalingRatio) {
            out.append(format("%-6s%20.2f%n", "mean", histogram.getMean() / outputScalingRatio));
            out.append(format("%-6s%20.2f%n", "min", histogram.getMinValue() / outputScalingRatio));
            out.append(format("%-6s%20.2f%n", "50.00%", histogram.getValueAtPercentile(50.0d) / outputScalingRatio));
            out.append(format("%-6s%20.2f%n", "90.00%", histogram.getValueAtPercentile(90.0d) / outputScalingRatio));
            out.append(format("%-6s%20.2f%n", "99.00%", histogram.getValueAtPercentile(99.0d) / outputScalingRatio));
            out.append(format("%-6s%20.2f%n", "99.90%", histogram.getValueAtPercentile(99.9d) / outputScalingRatio));
            out.append(format("%-6s%20.2f%n", "99.99%", histogram.getValueAtPercentile(99.99d) / outputScalingRatio));
            out.append(format("%-6s%20.2f%n", "max", histogram.getMaxValue() / outputScalingRatio));
            out.append(format("%-6s%20d%n", "count", histogram.getTotalCount()));
         }
      }, SHORT {
         @Override
         public void output(final Histogram histogram, final PrintStream out, double outputScalingRatio) {
            out.append(format("%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%d%n", histogram.getMean() / outputScalingRatio, histogram.getMinValue() / outputScalingRatio, histogram.getValueAtPercentile(50.0d) / outputScalingRatio, histogram.getValueAtPercentile(90.0d) / outputScalingRatio, histogram.getValueAtPercentile(99.0d) / outputScalingRatio, histogram.getValueAtPercentile(99.9d) / outputScalingRatio, histogram.getValueAtPercentile(99.99d) / outputScalingRatio, histogram.getMaxValue() / outputScalingRatio, histogram.getTotalCount()));
         }
      }, DETAIL {
         @Override
         public void output(final Histogram histogram, final PrintStream out, double outputScalingRatio) {
            histogram.outputPercentileDistribution(out, outputScalingRatio);
         }
      };

      public abstract void output(final Histogram histogram, final PrintStream out, double outputScalingRatio);
   }


   private void testSharedDurableConsumer2(Connection connection1,
                                           Connection connection2) throws JMSException, InterruptedException {
      OutputFormat format = OutputFormat.LONG;
      ExecutorService executorService = Executors.newCachedThreadPool();
      connection1.start();
      connection2.start();

      int count = 10_000_000;
      int producers = 1;
      int consumers = producers * 2;
      final long HIGHEST_TRACKABLE_VALUE = TimeUnit.MINUTES.toNanos(1);
      final SingleWriterRecorder[] recorders = new SingleWriterRecorder[consumers];
      final CountDownLatch consumersFinished = new CountDownLatch(consumers);
      final CountDownLatch consumersStarted = new CountDownLatch(consumers);
      for (int i = 0; i < consumers; i++) {
         final int id = i;
         recorders[id] = new SingleWriterRecorder(HIGHEST_TRACKABLE_VALUE, 2);
         executorService.submit(() -> {
            try {
               final long totalMessagedToBeReceived = producers * count;
               final SingleWriterRecorder recorder = recorders[id];
               try (Session session2 = connection2.createSession(false, ActiveMQJMSConstants.PRE_ACKNOWLEDGE);
                    MessageConsumer consumer1 = session2.createSharedDurableConsumer(session2.createTopic(getTopicName()), "SharedConsumer")) {
                  consumersStarted.countDown();
                  for (int r = 0; r < totalMessagedToBeReceived; r++) {
                     BytesMessage message;
                     while ((message = (BytesMessage) consumer1.receive(1000)) == null) {

                     }
                     long end = System.nanoTime();
                     long start = message.readLong();
                     recorder.recordValue(end - start);
                  }
               } catch (Exception e) {
                  System.out.println(e);
               }
            } finally {
               consumersFinished.countDown();
            }

         });
      }
      consumersStarted.await();
      final AtomicBoolean finished = new AtomicBoolean();
      executorService.submit(()->{
         final Histogram snapshots[] = new Histogram[consumers];
         try {
            while(!finished.get()) {
               TimeUnit.SECONDS.sleep(1);
               for (int i = 0; i < consumers; i++) {
                  snapshots[i] = recorders[i].getIntervalHistogram(snapshots[i]);
               }
               for (int i = 0; i< consumers; i ++) {
                  final Histogram histogram = snapshots[i];
                  if (histogram != null) {
                     System.out.println("[" + (i + 1) + "] - us latencies:");
                     //in us
                     format.output(histogram, System.out, 1000d);
                     System.out.println();
                     snapshots[i] = histogram;
                  } else {
                     System.out.println("[" + (i + 1) + "] - us latencies: NOT AVAILABLE");
                  }
               }
            }
         } catch (InterruptedException e) {
            e.printStackTrace();
         }

      });
      CountDownLatch producerFinished = new CountDownLatch(producers);
      for (int j = 0; j < producers; j++) {
         executorService.submit(() -> {
            try (Session session1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
                 MessageProducer producer = session1.createProducer(session1.createTopic(getTopicName()))) {
               producer.setDisableMessageID(true);
               producer.setDisableMessageTimestamp(true);
               final BytesMessage message = session1.createBytesMessage();
               producer.setDeliveryMode(DeliveryMode.PERSISTENT);
               for (int i = 0; i < count; i++) {
                  try {
                     message.clearBody();
                     final long sendTime = System.nanoTime();
                     message.writeLong(sendTime);
                     producer.send(message);
                     i++;
                  } catch (JMSException e) {
                     System.out.println(e);
                  }
               }
            } catch (JMSException e) {
               e.printStackTrace();
            } finally {
               producerFinished.countDown();
            }
         });

         try {
            producerFinished.await();
         } catch (InterruptedException e) {
            e.printStackTrace();
         }
         try {
            consumersFinished.await();
         } catch (InterruptedException e) {
            e.printStackTrace();
         }
         finished.lazySet(true);
         executorService.shutdown();
         try {
            executorService.awaitTermination(1, TimeUnit.SECONDS);
         } catch (InterruptedException e) {
            e.printStackTrace();
         }
         executorService.shutdownNow();
      }
   }

   //   @Test
   //   public void testSharedDurableConsumer() throws Exception {
   //      Connection connection = createConnection(); //AMQP
   //      Connection connection2 = createConnection(); //AMQP
   //
   //      testSharedDurableConsumer(connection, connection2);
   //   }
   //
   //   @Test
   //   public void testSharedDurableConsumerWithArtemisClient() throws Exception {
   //
   //      Connection connection = createCoreConnection(); //CORE
   //      Connection connection2 = createCoreConnection(); //CORE
   //
   //      testSharedDurableConsumer(connection, connection2);
   //
   //   }

   @Test
   public void testSharedDurableConsumerWithInVMClient() throws Exception {

      Connection connection = createInVMConnection(); //CORE
      Connection connection2 = createInVMConnection(); //CORE

      testSharedDurableConsumer(connection, connection2);

   }

   //   @Test
   //   public void testSharedDurableConsumerWithAMQPClientAndArtemisClient() throws Exception {
   //
   //      Connection connection = createConnection(); //AMQP
   //      Connection connection2 = createCoreConnection(); //CORE
   //
   //      testSharedDurableConsumer(connection, connection2);
   //
   //   }
   //
   //   @Test
   //   public void testSharedDurableConsumerWithArtemisClientAndAMQPClient() throws Exception {
   //
   //      Connection connection = createCoreConnection(); //CORE
   //      Connection connection2 = createConnection(); //AMQP
   //
   //      testSharedDurableConsumer(connection, connection2);
   //
   //   }
}



