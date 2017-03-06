/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.crossprotocol;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.Session;
import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQXAConnectionFactory;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.jms.server.JMSServerManager;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpMessage;
import org.apache.activemq.transport.amqp.client.AmqpSender;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.messaging.Header;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AMQPToOpenwireTest extends ActiveMQTestBase {

   public static final String OWHOST = "localhost";
   public static final int OWPORT = 61616;
   protected static final String urlString = "tcp://" + OWHOST + ":" + OWPORT + "?wireFormat.cacheEnabled=true";

   JMSServerManager serverManager;
   private ActiveMQServer server;
   protected ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(urlString);
   protected ActiveMQXAConnectionFactory xaFactory = new ActiveMQXAConnectionFactory(urlString);
   private JmsConnectionFactory qpidfactory;
   protected String queueName = "amqTestQueue1";
   private SimpleString coreQueue;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      server = createServer(true, true);
      server.start();
      server.waitForActivation(10, TimeUnit.SECONDS);

      Configuration serverConfig = server.getConfiguration();
      serverConfig.getAddressesSettings().put("#", new AddressSettings().setAutoCreateQueues(false).setAutoCreateAddresses(false).setDeadLetterAddress(new SimpleString("ActiveMQ.DLQ")));
      serverConfig.setSecurityEnabled(false);
      coreQueue = new SimpleString(queueName);
      server.createQueue(coreQueue, RoutingType.ANYCAST, coreQueue, null, false, false);
      qpidfactory = new JmsConnectionFactory("amqp://localhost:61616");
   }

   @Override
   @After
   public void tearDown() throws Exception {
      server.stop();
   }

   @Test
   public void testObjectMessage() throws Exception {
      Connection connection = null;
      try {
         connection = qpidfactory.createConnection();
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue queue = session.createQueue(coreQueue.toString());
         MessageProducer producer = session.createProducer(queue);
         ArrayList list = new ArrayList();
         list.add("aString");
         ObjectMessage objectMessage = session.createObjectMessage(list);
         producer.send(objectMessage);
         connection.close();

         connection = factory.createConnection();
         session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         queue = session.createQueue(queueName);
         MessageConsumer consumer = session.createConsumer(queue);
         connection.start();
         BytesMessage receive = (BytesMessage) consumer.receive(5000);
         assertNotNull(receive);
         byte[] bytes = new byte[(int) receive.getBodyLength()];
         receive.readBytes(bytes);
         ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes));
         list = (ArrayList) ois.readObject();
         assertEquals(list.get(0), "aString");
         connection.close();
      } catch (Exception e) {
         e.printStackTrace();
      } finally {
         if (connection != null) {
            connection.close();
         }
      }
   }

   @Test
   public void testDeliveryCountMessage() throws Exception {
      AmqpClient client = new AmqpClient(new URI("tcp://127.0.0.1:61616"), null, null);
      AmqpConnection amqpconnection = client.connect();
      try {
         AmqpSession session = amqpconnection.createSession();
         AmqpSender sender = session.createSender(queueName);
         AmqpMessage message = new AmqpMessage();
         message.setMessageId("MessageID:" + 0);
         message.getWrappedMessage().setHeader(new Header());
         message.getWrappedMessage().getHeader().setDeliveryCount(new UnsignedInteger(2));
         sender.send(message);
      } finally {
         amqpconnection.close();
      }

      Connection connection = null;
      try {
         connection = factory.createConnection();
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue queue = session.createQueue(queueName);
         MessageConsumer consumer = session.createConsumer(queue);
         connection.start();
         Message receive = consumer.receive(5000);
         assertNotNull(receive);
      } finally {
         if (connection != null) {
            connection.close();
         }
      }
   }
}
