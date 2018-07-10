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

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.jms.client.ActiveMQDestination;
import org.apache.activemq.artemis.jms.client.ActiveMQQueue;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JMSMessageGroupsTest extends JMSClientTestSupport {

   protected static final Logger LOG = LoggerFactory.getLogger(JMSMessageGroupsTest.class);

   private static final int ITERATIONS = 10;
   private static final int MESSAGE_COUNT = 10;
   private static final int MESSAGE_SIZE = 10 * 1024;
   private static final int RECEIVE_TIMEOUT = 3000;
   private static final String JMSX_GROUP_ID = "JmsGroupsTest";

   @Test(timeout = 60000)
   public void testGroupSeqIsNeverLost() throws Exception {
      AtomicInteger sequenceCounter = new AtomicInteger();

      for (int i = 0; i < ITERATIONS; ++i) {
         Connection connection = createConnection();
         try {
            sendMessagesToBroker(connection, MESSAGE_COUNT, sequenceCounter);
            readMessagesOnBroker(connection, MESSAGE_COUNT);
         } finally {
            connection.close();
         }
      }
   }

   @Test
   public void testManyMessagesDeliveredToOnlyOneConsumerNoGroup() throws Exception {
      Connection connection = createConnection();
      try {
         ActiveMQDestination destination = new ActiveMQQueue("TEST");
         // Setup a first connection
         connection.start();
         final int test = 10;
         for (int t = 0; t < test; t++) {
            System.out.println("Test " + (t + 1) + " started");
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer1 = session.createConsumer(destination);
            MessageProducer producer = session.createProducer(destination);
            final int messages = 5_000;
            // Send the messages.
            for (int i = 0; i < messages; i++) {
               TextMessage message = session.createTextMessage("message " + i);
               LOG.info("sending message: " + message);
               producer.send(message);
            }
            System.out.println("sent all messages: start receiving in 10 seconds");
            Thread.sleep(10_000);
            System.out.println("start receiving");

            // All the messages should have been sent down connection 1.. just get
            // the first 3
            for (int i = 0; i < messages; i++) {
               TextMessage m1 = (TextMessage) consumer1.receive(500);
               assertNotNull("m1 is null for index: " + i, m1);
            }
            System.out.println("received all messages");
            // Close the first consumer.
            consumer1.close();

            session.close();
         }
      } finally {
         connection.close();
      }
   }

   @Test
   public void testManyMessagesDeliveredToOnlyOneConsumer() throws Exception {
      Connection connection = createConnection();
      try {
         ActiveMQDestination destination = new ActiveMQQueue("TEST");
         // Setup a first connection
         connection.start();
         final int test = 10;
         for (int t = 0; t < test; t++) {
            System.out.println("Test " + (t + 1) + " started");
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer1 = session.createConsumer(destination);
            MessageConsumer consumer2 = session.createConsumer(destination);
            MessageConsumer consumer3 = session.createConsumer(destination);
            MessageProducer producer = session.createProducer(destination);
            final int messages = 5_000;
            // Send the messages.
            for (int i = 0; i < messages; i++) {
               TextMessage message = session.createTextMessage("message " + i);
               message.setStringProperty("JMSXGroupID", "TEST-GROUP");
               message.setIntProperty("JMSXGroupSeq", i + 1);
               LOG.info("sending message: " + message);
               producer.send(message);
            }
            System.out.println("sent all messages: start receiving in 10 seconds");
            Thread.sleep(10_000);
            System.out.println("start receiving");

            // All the messages should have been sent down connection 1.. just get
            // the first 3
            for (int i = 0; i < messages; i++) {
               TextMessage m1 = (TextMessage) consumer1.receive(500);
               assertNotNull("m1 is null for index: " + i, m1);
               assertEquals(m1.getIntProperty("JMSXGroupSeq"), i + 1);
            }
            System.out.println("received all messages");
            // Close the first consumer.
            consumer1.close();
            consumer2.close();
            consumer3.close();

            session.close();
         }
      } finally {
         connection.close();
      }
   }

   protected void readMessagesOnBroker(Connection connection, int count) throws Exception {
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Queue queue = session.createQueue(getQueueName());
      MessageConsumer consumer = session.createConsumer(queue);

      for (int i = 0; i < MESSAGE_COUNT; ++i) {
         Message message = consumer.receive(RECEIVE_TIMEOUT);
         assertNotNull(message);
         LOG.debug("Read message #{}: type = {}", i, message.getClass().getSimpleName());
         String gid = message.getStringProperty("JMSXGroupID");
         String seq = message.getStringProperty("JMSXGroupSeq");
         LOG.debug("Message assigned JMSXGroupID := {}", gid);
         LOG.debug("Message assigned JMSXGroupSeq := {}", seq);
      }

      session.close();
   }

   protected void sendMessagesToBroker(Connection connection, int count, AtomicInteger sequence) throws Exception {
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Queue queue = session.createQueue(getQueueName());
      MessageProducer producer = session.createProducer(queue);

      byte[] buffer = new byte[MESSAGE_SIZE];
      for (count = 0; count < MESSAGE_SIZE; count++) {
         String s = String.valueOf(count % 10);
         Character c = s.charAt(0);
         int value = c.charValue();
         buffer[count] = (byte) value;
      }

      LOG.debug("Sending {} messages to destination: {}", MESSAGE_COUNT, queue);
      for (int i = 1; i <= MESSAGE_COUNT; i++) {
         BytesMessage message = session.createBytesMessage();
         message.setJMSDeliveryMode(DeliveryMode.PERSISTENT);
         message.setStringProperty("JMSXGroupID", JMSX_GROUP_ID);
         message.setIntProperty("JMSXGroupSeq", sequence.incrementAndGet());
         message.writeBytes(buffer);
         producer.send(message);
      }

      session.close();
   }
}
