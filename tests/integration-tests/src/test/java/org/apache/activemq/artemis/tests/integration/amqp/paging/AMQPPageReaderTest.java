package org.apache.activemq.artemis.tests.integration.amqp.paging;

import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.UUID;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.paging.cursor.impl.PageReaderTest;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPStandardMessage;
import org.apache.activemq.artemis.protocol.amqp.util.NettyWritable;
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.UnsignedByte;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.Header;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.amqp.messaging.Properties;
import org.apache.qpid.proton.message.impl.MessageImpl;

public class AMQPPageReaderTest extends PageReaderTest {

   @Override
   protected Message createMessage(SimpleString address, int msgId, byte[] content) {
      final MessageImpl protonMessage = createProtonMessage(address.toString(), content);
      final AMQPStandardMessage msg = encodeAndDecodeMessage(protonMessage, content.length);
      msg.setAddress(address);
      msg.setMessageID(msgId);
      return msg;
   }

   private AMQPStandardMessage encodeAndDecodeMessage(MessageImpl message, int expectedSize) {
      ByteBuf nettyBuffer = Unpooled.buffer(expectedSize);

      message.encode(new NettyWritable(nettyBuffer));
      byte[] bytes = new byte[nettyBuffer.writerIndex()];
      nettyBuffer.readBytes(bytes);

      return new AMQPStandardMessage(0, bytes, null);
   }

   private MessageImpl createProtonMessage(String address, byte[] content) {
      MessageImpl message = (MessageImpl) Proton.message();

      Header header = new Header();
      header.setDurable(true);
      header.setPriority(UnsignedByte.valueOf((byte) 9));

      Properties properties = new Properties();
      properties.setCreationTime(new Date(System.currentTimeMillis()));
      properties.setTo(address);
      properties.setMessageId(UUID.randomUUID());

      MessageAnnotations annotations = new MessageAnnotations(new LinkedHashMap<>());
      ApplicationProperties applicationProperties = new ApplicationProperties(new LinkedHashMap<>());

      AmqpValue body = new AmqpValue(Arrays.copyOf(content, content.length));

      message.setHeader(header);
      message.setMessageAnnotations(annotations);
      message.setProperties(properties);
      message.setApplicationProperties(applicationProperties);
      message.setBody(body);

      return message;
   }
}
