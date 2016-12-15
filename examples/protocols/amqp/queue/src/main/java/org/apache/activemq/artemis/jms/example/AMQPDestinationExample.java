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

package org.apache.activemq.artemis.jms.example;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import java.util.concurrent.locks.LockSupport;

import org.apache.qpid.jms.JmsConnectionFactory;

public class AMQPDestinationExample {

   public static void main(String[] args) throws Exception {
      boolean queue = false;
      final int messages = 100_000;
      Connection connection = null;
      ConnectionFactory connectionFactory = new JmsConnectionFactory("amqp://localhost:5672");

      try {

         // Step 1. Create an amqp qpid 1.0 connection
         connection = connectionFactory.createConnection();
         connection.start();

         // Step 2. Create a session
         Session session = null;
         try {
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final Session currentSession = session;
            // Step 3. Create a sender
            final Destination destination;
            if (queue) {
               destination = session.createQueue("example");
            } else {
               destination = session.createTopic("example");
            }

            final Thread consumerThread = new Thread(() -> {
               MessageConsumer consumer = null;
               try {
                  // Step 5. create a moving receiver, this means the message will be removed from the queue
                  consumer = currentSession.createConsumer(destination);

                  for (int i = 0; i < messages; i++) {
                     Message message;
                     while ((message = consumer.receiveNoWait()) == null) {
                        LockSupport.parkNanos(1L);
                     }
                  }
                  System.out.println("finished consuming messages");
                  long count = 0;
                  Message message;
                  while ((message = consumer.receiveNoWait()) != null) {
                     count++;
                     if (count % 1000 == 0) {
                        System.err.println("received " + count + " more messages, possible errors!");
                     }
                  }
                  if (count > 0) {
                     System.err.println("received " + count + " more messages, possible errors!");
                  }
               } catch (Throwable throwable) {
                  System.err.println(throwable);
               } finally {
                  if (consumer != null) {
                     try {
                        consumer.close();
                     } catch (Throwable throwable) {

                     }
                  }
               }
            });
            consumerThread.start();

            MessageProducer sender = null;
            try {
               sender = session.createProducer(destination);
               sender.setDisableMessageTimestamp(true);
               final BytesMessage message = session.createBytesMessage();
               for (int i = 0; i < messages; i++) {
                  sender.send(message);
               }
            } catch (Throwable t) {
               System.err.println(t);
            } finally {
               if (sender != null) {
                  sender.close();
               }
            }
            System.out.println("...finishing producing, wait consumer thread...");
            consumerThread.join();
            System.out.println("...finished consumer thread.");
         } catch (Throwable t) {
            System.err.println(t);
         } finally {
            if (session != null) {
               session.close();
            }
         }
      } finally {
         if (connection != null) {
            // Step 9. close the connection
            connection.close();
         }
      }
   }
}
