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
package org.apache.activemq.artemis.tests.integration.client;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.Header;
import io.aeron.shadow.org.HdrHistogram.Histogram;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledUnsafeDirectByteBuf;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQExceptionType;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.message.impl.CoreMessage;
import org.apache.activemq.artemis.core.message.impl.CoreMessageObjectPools;
import org.apache.activemq.artemis.core.postoffice.impl.AeronConnector;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Before;
import org.junit.Test;

public class CreateQueueTest extends ActiveMQTestBase {

   public final SimpleString addressA = new SimpleString("addressA");
   public final SimpleString addressB = new SimpleString("addressB");
   public final SimpleString queueA = new SimpleString("queueA");
   public final SimpleString queueB = new SimpleString("queueB");
   public final SimpleString queueC = new SimpleString("queueC");
   public final SimpleString queueD = new SimpleString("queueD");

   private ServerLocator locator;
   private ActiveMQServer server;
   private ClientSessionFactory cf;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      locator = createInVMNonHALocator();
      server = createServer(false);

      server.start();
      cf = createSessionFactory(locator);
   }

   @Test
   public void sendReceiveWithAeron() throws InterruptedException {
      final boolean IPC = false;
      final int tests = 20;
      final int messages = 100_000;
      final MediaDriver.Context context = new MediaDriver.Context();
      MediaDriver mediaDriver = null;
      if (!IPC) {
         context.aeronDirectoryName("/dev/shm/client");
         context.threadingMode(ThreadingMode.SHARED);
         mediaDriver = MediaDriver.launchEmbedded(context);
      } else {
         context.aeronDirectoryName(AeronConnector.EMBEDDED_DIR_NAME);
      }
      try {
         final Aeron.Context ctx = new Aeron.Context();
         //use the temps dir of my machine
         ctx.aeronDirectoryName(context.aeronDirectoryName());
         try (Aeron aeron = Aeron.connect(ctx)) {
            final ExclusivePublication publication = aeron.addExclusivePublication(AeronConnector.INCOMING_CHANNEL, AeronConnector.INCOMING_STREAM_ID);
            final Subscription subscription = aeron.addSubscription(AeronConnector.OUTGOING_CHANNEL, AeronConnector.OUTGOING_STREAM_ID);
            final CountDownLatch startedConsumer = new CountDownLatch(1);
            final CountDownLatch[] finishedTests = new CountDownLatch[tests];
            for (int t = 0; t < tests; t++) {
               finishedTests[t] = new CountDownLatch(1);
            }
            final Thread consumerThreads = new Thread(() -> {
               startedConsumer.countDown();
               for (int t = 0; t < tests; t++) {
                  int remaining = messages;
                  while (remaining > 0) {
                     int polled = subscription.poll(CreateQueueTest::onFragmentReceived, remaining);
                     if (polled <= 0) {
                        Thread.yield();
                     } else {
                        remaining -= polled;
                     }
                  }
                  synchronized (System.out) {
                     System.out.println("RESPONSE TIME in us");
                     HISTOGRAM.outputPercentileDistribution(System.out, 1000d);
                  }
                  HISTOGRAM.reset();
                  finishedTests[t].countDown();
               }
            });
            final ByteBuf sentBuffer = Unpooled.buffer(1024);
            final UnsafeBuffer sentMessageBuffer = new UnsafeBuffer();
            final int msgSize = 100;
            final byte[] bytes = new byte[msgSize];
            consumerThreads.start();
            startedConsumer.await();
            long msgId = 1;
            final Histogram waitTime = new Histogram(TimeUnit.MINUTES.toNanos(2), 2);
            final Histogram encodeTime = new Histogram(TimeUnit.MINUTES.toNanos(2), 2);
            for (int t = 0; t < tests; t++) {
               final long start = System.nanoTime();
               for (int m = 0; m < messages; m++) {
                  final CoreMessage msg = new CoreMessage();
                  sentBuffer.clear();
                  msg.setBuffer(sentBuffer);
                  msg.setMessageID(msgId);
                  msgId++;
                  msg.setDurable(false);
                  msg.setRoutingType(RoutingType.ANYCAST);
                  msg.setAddress("aeron.queue");
                  msg.putBytesProperty("bytes", bytes);
                  msg.putLongProperty("t", System.nanoTime());
                  final long startEncode = System.nanoTime();
                  final int encodeSize = msg.getEncodeSize();
                  sentMessageBuffer.wrap(sentBuffer.array(), sentBuffer.arrayOffset(), encodeSize);
                  try {
                     final long startOffer = System.nanoTime();
                     while (publication.offer(sentMessageBuffer) < 0) {
                        Thread.yield();
                     }
                     final long elapsedOffer = System.nanoTime() - startOffer;
                     waitTime.recordValue(elapsedOffer);
                     encodeTime.recordValue(startOffer - startEncode);
                  } finally {
                     sentMessageBuffer.wrap(0, 0);
                  }
               }
               finishedTests[t].await();
               final long elapsed = System.nanoTime() - start;
               System.out.println(messages * 1000_000_000L / elapsed + " msg/sec");
               synchronized (System.out) {
                  System.out.println("WAIT TIME in us");
                  waitTime.outputPercentileDistribution(System.out, 1000d);
               }
               waitTime.reset();
               synchronized (System.out) {
                  System.out.println("ENCODE TIME in us");
                  encodeTime.outputPercentileDistribution(System.out, 1000d);
               }
               encodeTime.reset();
               Thread.sleep(2000);
            }
         }
      } catch (Throwable t) {
         t.printStackTrace();
      } finally {
         if (mediaDriver != null) {
            mediaDriver.close();
         }
      }
   }

   private final static Histogram HISTOGRAM = new Histogram(TimeUnit.MINUTES.toNanos(2),2);
   private final static CoreMessageObjectPools pools = new CoreMessageObjectPools();

   private static void onFragmentReceived(DirectBuffer buffer, int offset, int length, Header header) {
      final long arrived = System.nanoTime();
      final ByteBuf unpooledBytes = Unpooled.buffer(length, length);
      unpooledBytes.ensureWritable(length);
      buffer.getBytes(offset, unpooledBytes.array(), 0, length);
      unpooledBytes.writerIndex(length);
      final CoreMessage arrivedMessage = new CoreMessage(pools);
      arrivedMessage.receiveBuffer(unpooledBytes);
      final long elapsed = arrived - arrivedMessage.getLongProperty("t");
      HISTOGRAM.recordValue(elapsed);
   }

   @Test
   public void testUnsupportedRoutingType() throws Exception {
      ClientSession sendSession = cf.createSession(false, true, true);
      server.getAddressSettingsRepository().addMatch(addressA.toString(), new AddressSettings().setAutoCreateAddresses(false));
      server.getAddressSettingsRepository().addMatch(addressB.toString(), new AddressSettings().setAutoCreateAddresses(false));

      EnumSet<RoutingType> routingTypes = EnumSet.of(RoutingType.ANYCAST);
      sendSession.createAddress(addressA, routingTypes, false);
      try {
         sendSession.createQueue(addressA, RoutingType.MULTICAST, queueA);
         fail("Creating a queue here should fail since the queue routing type differs from what is supported on the address.");
      } catch (Exception e) {
         assertTrue(e instanceof ActiveMQException);
         ActiveMQException ae = (ActiveMQException) e;
         assertEquals(ActiveMQExceptionType.INTERNAL_ERROR, ae.getType());
      }

      routingTypes = EnumSet.of(RoutingType.MULTICAST);
      sendSession.createAddress(addressB, routingTypes, false);
      try {
         sendSession.createQueue(addressB, RoutingType.ANYCAST, queueB);
         fail("Creating a queue here should fail since the queue routing type differs from what is supported on the address.");
      } catch (Exception e) {
         assertTrue(e instanceof ActiveMQException);
         ActiveMQException ae = (ActiveMQException) e;
         assertEquals(ActiveMQExceptionType.INTERNAL_ERROR, ae.getType());
      }
      sendSession.close();
   }

   @Test
   public void testAddressDoesNotExist() throws Exception {
      ClientSession sendSession = cf.createSession(false, true, true);
      server.getAddressSettingsRepository().addMatch(addressA.toString(), new AddressSettings().setAutoCreateAddresses(false));
      Set<RoutingType> routingTypes = new HashSet<>();
      routingTypes.add(RoutingType.ANYCAST);
      try {
         sendSession.createQueue(addressA, RoutingType.MULTICAST, queueA);
         fail("Creating a queue here should fail since the queue's address doesn't exist and auto-create-addresses = false.");
      } catch (Exception e) {
         assertTrue(e instanceof ActiveMQException);
         ActiveMQException ae = (ActiveMQException) e;
         assertEquals(ActiveMQExceptionType.ADDRESS_DOES_NOT_EXIST, ae.getType());
      }
      sendSession.close();
   }
}
