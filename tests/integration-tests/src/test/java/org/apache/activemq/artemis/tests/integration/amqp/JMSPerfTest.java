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

import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSConstants;
import org.apache.activemq.artemis.core.remoting.impl.invm.TransportConstants;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.junit.Test;

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

   private void testSharedDurableConsumer(Connection connection1, Connection connection2) throws JMSException {
      try {
         for (int i = 0; i < 1; i++)
            testSharedDurableConsumer2(connection1, connection2);
      } finally {
         connection1.close();
         connection2.close();
      }
   }

   private void testSharedDurableConsumer2(Connection connection1, Connection connection2) throws JMSException {
      ExecutorService executorService = Executors.newCachedThreadPool();
      try {

         connection1.start();
         connection2.start();

         AtomicBoolean running = new AtomicBoolean(true);

         int count = 10_000_000;
         int per = 10_000;
         int producers = 1;
         int consumers = producers * 2;

         AtomicInteger counter = new AtomicInteger();
         AtomicLong endTime = new AtomicLong(0);
         long totalStart = System.nanoTime();
         for (int i = 0; i < consumers; i++) {
            executorService.submit(() -> {
               long total = 0;
               long num = 0;
               try (Session session2 = connection2.createSession(false, ActiveMQJMSConstants.PRE_ACKNOWLEDGE); MessageConsumer consumer1 = session2.createSharedDurableConsumer(session2.createTopic(getTopicName()), "SharedConsumer")) {
                  while (running.get()) {
                     Message message1 = consumer1.receive(1000);
                     if (message1 != null) {
                        long end = System.nanoTime();
                        long start = message1.getLongProperty("time");
                        total = total + (end - start);
                        num = num + 1;
                        if (num % per == 0) {
                           System.out.println("RESULT: " + TimeUnit.NANOSECONDS.toMicros(total / num));
                           total = 0;
                           num = 0;
                        }
                        if (counter.incrementAndGet() == count) {
                           endTime.set(System.nanoTime());
                        }
                     }
                  }

                  try {
                     System.out.println("RESULT: " + TimeUnit.NANOSECONDS.toMicros(total / num));
                  } catch (Throwable t) {
                     System.out.println("eeel");
                  }

                  //               session2.unsubscribe("SharedConsumer");

               } catch (Exception e) {
                  System.out.println(e);
               }

            });
         }

         CountDownLatch countDownLatch = new CountDownLatch(producers);
         for (int j = 0; j < producers; j++) {
            executorService.submit(() -> {
               try (Session session1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE); MessageProducer producer = session1.createProducer(session1.createTopic(getTopicName()));) {
                  producer.setDeliveryMode(DeliveryMode.PERSISTENT);
                  int i = 0;
                  while (i <= count) {
                     try {
                        TextMessage message = session1.createTextMessage("hello");
                        message.setLongProperty("time", System.nanoTime());
                        producer.send(message);
                        i++;
                        if (i % 10000 == 0 && i + 10000 > counter.get()) {
                           try {
                              Thread.sleep(1000);
                           } catch (InterruptedException e) {
                              e.printStackTrace();
                           }
                        }
                     } catch (JMSException e) {
                        System.out.println(e);
                     }
                  }
               } catch (JMSException jmse) {
                  jmse.printStackTrace();
               } finally {
                  countDownLatch.countDown();
               }

            });
         }

         try {
            countDownLatch.await();
         } catch (InterruptedException e) {
            e.printStackTrace();
         }
         try {
            Thread.sleep(1000);
         } catch (InterruptedException e) {
            e.printStackTrace();
         }

         long totalEnd = endTime.get() == 0 ? System.nanoTime() : endTime.get();

         long cnt = counter.get();

         executorService.shutdown();
         try {
            executorService.awaitTermination(1, TimeUnit.SECONDS);
         } catch (InterruptedException e) {
            e.printStackTrace();
         }
         executorService.shutdownNow();

      } finally {
         //         connection1.close();
         //         connection2.close();
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



