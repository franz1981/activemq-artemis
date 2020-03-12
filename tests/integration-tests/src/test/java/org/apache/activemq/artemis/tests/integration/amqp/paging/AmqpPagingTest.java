package org.apache.activemq.artemis.tests.integration.amqp.paging;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.tests.integration.amqp.AmqpClientTestSupport;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpMessage;
import org.apache.activemq.transport.amqp.client.AmqpReceiver;
import org.apache.activemq.transport.amqp.client.AmqpSender;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.junit.Assert;
import org.junit.Test;

public class AmqpPagingTest extends AmqpClientTestSupport {

   @Override
   protected void addConfiguration(ActiveMQServer server) {
      super.addConfiguration(server);
      final Map<String, AddressSettings> addressesSettings = server.getConfiguration().getAddressesSettings();
      addressesSettings.get("#").setMaxSizeBytes(100000).setPageSizeBytes(10000);
   }

   @Test(timeout = 60000)
   public void testPaging() throws Exception {
      final int MSG_SIZE = 1000;
      final StringBuilder builder = new StringBuilder();
      for (int i = 0; i < MSG_SIZE; i++) {
         builder.append('0');
      }
      final String data = builder.toString();
      final int MSG_COUNT = 5000;

      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());

      sendMessages(getQueueName(), MSG_COUNT, null, false, Collections.emptyMap(), id -> data);
      AmqpSession session = connection.createSession();

      AmqpReceiver receiver = session.createReceiver(getQueueName());
      receiver.flow(MSG_COUNT);
      for (int i = 0; i < MSG_COUNT; ++i) {
         AmqpMessage receive = receiver.receive(10, TimeUnit.SECONDS);
         assertNotNull("Not received anything after " + i + " receive", receive);
      }
      receiver.close();
      connection.close();
   }

}
