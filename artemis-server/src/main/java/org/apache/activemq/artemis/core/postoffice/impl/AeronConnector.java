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

package org.apache.activemq.artemis.core.postoffice.impl;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.aeron.Aeron;
import io.aeron.CommonContext;
import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.message.impl.CoreMessage;
import org.apache.activemq.artemis.core.postoffice.PostOffice;
import org.apache.activemq.artemis.core.postoffice.RoutingStatus;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Consumer;
import org.apache.activemq.artemis.core.server.HandleStatus;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;

public final class AeronConnector implements AutoCloseable {

   public static final String EMBEDDED_DIR_NAME = "/dev/shm/broker";
   //public static final String INCOMING_CHANNEL = "aeron:udp?endpoint=localhost:40123";
   public static final String INCOMING_CHANNEL = CommonContext.IPC_CHANNEL;
   //public static final String OUTGOING_CHANNEL = "aeron:udp?endpoint=localhost:40124";
   public static final String OUTGOING_CHANNEL = CommonContext.IPC_CHANNEL;
   public static final int INCOMING_STREAM_ID = 1;
   public static final int OUTGOING_STREAM_ID = 2;
   private final MediaDriver mediaDriver;
   private final Aeron aeron;
   private final Subscription incomingMessages;
   private final Thread dedicatedPollerOfIncomingMessages;
   private volatile boolean stop;
   private final CountDownLatch stopped;
   private final ExclusivePublication outgoingMessages;
   private final ActiveMQServer activeMQServer;

   public AeronConnector(ActiveMQServer activeMQServer) {
      final MediaDriver.Context context = new MediaDriver.Context();
      context.aeronDirectoryName(EMBEDDED_DIR_NAME);
      context.threadingMode(ThreadingMode.SHARED);
      mediaDriver = MediaDriver.launchEmbedded(context);
      final Aeron.Context ctx = new Aeron.Context();
      //use the temps dir of my machine
      ctx.aeronDirectoryName(mediaDriver.aeronDirectoryName());
      aeron = Aeron.connect(ctx);
      //it is unicast: it allows just a single sender and receiver
      //to have multiple senders on the same (single) receiver you would need
      //multicast
      incomingMessages = aeron.addSubscription(INCOMING_CHANNEL, INCOMING_STREAM_ID);
      outgoingMessages = aeron.addExclusivePublication(OUTGOING_CHANNEL, OUTGOING_STREAM_ID);
      dedicatedPollerOfIncomingMessages = new Thread(() -> {
         final FragmentHandler fragmentHandler = this::onFragment;
         while (!stop) {
            if (incomingMessages.poll(fragmentHandler, Integer.MAX_VALUE) == 0) {
               Thread.yield();
            }
         }
      });
      stopped = new CountDownLatch(1);
      this.activeMQServer = activeMQServer;
   }

   private final UnsafeBuffer outGoingMessage = new UnsafeBuffer();

   private void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
      final ByteBuf unpooledBytes = Unpooled.buffer(length, length);
      unpooledBytes.ensureWritable(length);
      buffer.getBytes(offset, unpooledBytes.array(), 0, length);
      unpooledBytes.writerIndex(length);
      try {
         final CoreMessage arrivedMessage = new CoreMessage();
         arrivedMessage.receiveBuffer(unpooledBytes);
         final SimpleString address = arrivedMessage.getAddressSimpleString();
         if (address != null) {
            try {
               final PostOffice postOffice = activeMQServer.getPostOffice();
               if (postOffice.route(arrivedMessage, true) != RoutingStatus.OK) {
                  createQueueAndConsumer(address, arrivedMessage);
                  postOffice.route(arrivedMessage, true);
               }
            } catch (Throwable t) {
               t.printStackTrace();
            }
         } else {
            System.err.println("ADDRESS NULL ON A RECEIVED MESSAGE!");
         }
      } catch (Throwable t) {
         t.printStackTrace();
      }
   }

   private void createQueueAndConsumer(SimpleString address, CoreMessage arrivedMessage) throws Exception {
      Queue queue = activeMQServer.createQueue(address, arrivedMessage.getRoutingType(), address, null, false, false);
      queue.addConsumer(new Consumer() {

         private final ByteBuf sentBuffer = Unpooled.buffer(1024);

         @Override
         public HandleStatus handle(MessageReference reference) throws Exception {
            return HandleStatus.HANDLED;
         }

         @Override
         public void proceedDeliver(MessageReference reference) throws Exception {
            final Message message = reference.getMessage();
            final int encodeSize = message.getEncodeSize();
            sentBuffer.clear().ensureWritable(encodeSize);
            message.sendBuffer(sentBuffer, 1);
            sentBuffer.writerIndex(encodeSize);
            outGoingMessage.wrap(sentBuffer.array());
            try {
               while (outgoingMessages.offer(outGoingMessage, 0, encodeSize) < 0) {
                  Thread.yield();
               }
               //acknowledge a message only when it has been offered to the aeron media (not 100% guarantee delivering)
               reference.acknowledge();
            } finally {
               outGoingMessage.wrap(0, 0);
            }
         }

         @Override
         public Filter getFilter() {
            return null;
         }

         @Override
         public List<MessageReference> getDeliveringMessages() {
            return Collections.emptyList();
         }

         @Override
         public String debug() {
            return null;
         }

         @Override
         public String toManagementString() {
            return null;
         }

         @Override
         public void disconnect() {

         }

         @Override
         public long sequentialID() {
            return 0;
         }
      });
   }

   public void start() {
      if (!stop) {
         dedicatedPollerOfIncomingMessages.start();
      }
   }

   @Override
   public void close() throws Exception {
      stop = true;
      stopped.await(10, TimeUnit.SECONDS);
      outgoingMessages.close();
      incomingMessages.close();
      aeron.close();
      mediaDriver.close();
   }
}