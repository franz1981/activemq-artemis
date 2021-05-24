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

import javax.jms.BytesMessage;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;
import java.util.function.LongSupplier;

import org.HdrHistogram.ValueRecorder;

public class ProducerThread extends Thread {

   protected final Session session;
   long target = -1;
   boolean verbose;
   long messageCount = 1000;
   Destination destination;
   int sleep = 0;
   boolean persistent = true;
   int messageSize = 0;
   int textMessageSize;
   int objectSize;
   long msgTTL = 0L;
   String msgGroupID = null;
   int transactionBatchSize;

   int transactions = 0;
   LongAdder sentCount;
   String message = null;
   String messageText = null;
   String payloadUrl = null;
   byte[] payload = null;
   volatile boolean running = true;
   ValueRecorder recorder;
   LongSupplier timeProvider;

   public ProducerThread(Session session, Destination destination, int threadNr) {
      super("Producer " + destination.toString() + ", thread=" + threadNr);
      this.destination = destination;
      this.session = session;
   }

   @Override
   public void run() {
      final LongSupplier timeProvider = this.timeProvider;
      final ValueRecorder recorder = this.recorder;
      final long target = this.target;
      long usPeriod = 0;
      if (target >= 0) {
         usPeriod = TimeUnit.SECONDS.toMicros(1) / target;
      }
      long expectedMessages = Long.MAX_VALUE;
      if (messageCount >= 0) {
         expectedMessages = messageCount;
      }
      long count = 0;
      MessageProducer producer = null;
      String threadName = Thread.currentThread().getName();
      try {
         producer = session.createProducer(destination);
         producer.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);
         producer.setTimeToLive(msgTTL);
         initPayLoad();
         System.out.println(threadName + " Started to calculate elapsed time ...\n");
         long tStart = System.currentTimeMillis();
         long nextExpectedUs = timeProvider.getAsLong();
         long nextTime = nextExpectedUs + usPeriod;
         while (running && count < expectedMessages) {
            final long sendTime;
            if (usPeriod > 0) {
               // accounting for coordinated omission
               sendTime = nextTime;
               nextTime = waitUntilNextTime(nextTime, usPeriod, timeProvider);
            } else {
               sendTime = timeProvider.getAsLong();
            }
            final long beforeSend = sendMessage(producer, threadName, count, sendTime);
            if (recorder != null) {
               final long afterSend = System.nanoTime();
               // this is including session::commit on transactions (that's ok!)
               recorder.recordValue(afterSend - beforeSend);
            }
            count++;
            sentCount.increment();
         }

         try {
            session.commit();
         } catch (Throwable ignored) {
         }

         System.out.println(threadName + " Produced: " + count + " messages");
         long tEnd = System.currentTimeMillis();
         long elapsed = (tEnd - tStart) / 1000;
         System.out.println(threadName + " Elapsed time in second : " + elapsed + " s");
         System.out.println(threadName + " Elapsed time in milli second : " + (tEnd - tStart) + " milli seconds");

      } catch (Exception e) {
         e.printStackTrace();
      } finally {
         if (producer != null) {
            try {
               producer.close();
            } catch (JMSException e) {
               e.printStackTrace();
            }
         }
      }
   }

   private static long waitUntilNextTime(final long nextTime,
                                         final long usPeriod,
                                         LongSupplier timeProvider) throws InterruptedException {
      final long now = timeProvider.getAsLong();
      TimeUnit.MICROSECONDS.sleep(nextTime - timeProvider.getAsLong());
      return now + usPeriod;
   }

   private long sendMessage(MessageProducer producer, String threadName, long count, long sendTime) throws Exception {
      Message message = createMessage(count, threadName);
      message.setLongProperty("_", sendTime);
      final long now = System.nanoTime();
      producer.send(message);
      if (verbose) {
         System.out.println(threadName + " Sent: " + (message instanceof TextMessage ? ((TextMessage) message).getText() : message.getJMSMessageID()));
      }

      if (transactionBatchSize > 0 && count > 0 && count % transactionBatchSize == 0) {
         if (verbose) {
            System.out.println(threadName + " Committing transaction: " + transactions++);
         }
         session.commit();
      }

      if (sleep > 0) {
         Thread.sleep(sleep);
      }
      return now;
   }

   private void initPayLoad() {
      if (messageSize > 0) {
         payload = new byte[messageSize];
         for (int i = 0; i < payload.length; i++) {
            payload[i] = '.';
         }
      }
   }

   protected Message createMessage(long i, String threadName) throws Exception {
      Message answer;
      if (payload != null) {
         BytesMessage msg = session.createBytesMessage();
         msg.writeBytes(payload);
         answer = msg;
      } else {
         if (textMessageSize > 0 || objectSize > 0) {

            if (objectSize > 0) {
               textMessageSize = objectSize;
            }
            if (messageText == null) {
               String read = readInputStream(getClass().getResourceAsStream(Producer.DEMO_TEXT), textMessageSize, i);
               if (read.length() == textMessageSize) {
                  messageText = read;
               } else {
                  StringBuilder buffer = new StringBuilder(read);
                  while (buffer.length() < textMessageSize) {
                     buffer.append(read);
                  }
                  messageText = buffer.toString();
               }

            }
         } else if (payloadUrl != null) {
            messageText = readInputStream(new URL(payloadUrl).openStream(), -1, i);
         } else if (message != null) {
            messageText = message;
         } else {
            messageText = createDefaultMessage(i);
         }

         if (objectSize > 0) {
            answer = session.createObjectMessage(messageText);
         } else {
            answer = session.createTextMessage(messageText);
         }
      }
      if ((msgGroupID != null) && (!msgGroupID.isEmpty())) {
         answer.setStringProperty("JMSXGroupID", msgGroupID);
      }

      answer.setLongProperty("count", i);
      answer.setStringProperty("ThreadSent", threadName);
      return answer;
   }

   private static String readInputStream(InputStream is, int size, long messageNumber) throws IOException {
      try (InputStreamReader reader = new InputStreamReader(is)) {
         char[] buffer;
         if (size > 0) {
            buffer = new char[size];
         } else {
            buffer = new char[1024];
         }
         int count;
         StringBuilder builder = new StringBuilder();
         while ((count = reader.read(buffer)) != -1) {
            builder.append(buffer, 0, count);
            if (size > 0)
               break;
         }
         return builder.toString();
      } catch (IOException ioe) {
         return createDefaultMessage(messageNumber);
      }
   }

   private static String createDefaultMessage(long messageNumber) {
      return "test message: " + messageNumber;
   }

   public ProducerThread setMessageCount(long messageCount) {
      this.messageCount = messageCount;
      return this;
   }

   public int getSleep() {
      return sleep;
   }

   public ProducerThread setSleep(int sleep) {
      this.sleep = sleep;
      return this;
   }

   public long getMessageCount() {
      return messageCount;
   }

   public boolean isPersistent() {
      return persistent;
   }

   public ProducerThread setPersistent(boolean persistent) {
      this.persistent = persistent;
      return this;
   }

   public ProducerThread setRunning(boolean running) {
      this.running = running;
      return this;
   }

   public long getMsgTTL() {
      return msgTTL;
   }

   public ProducerThread setMsgTTL(long msgTTL) {
      this.msgTTL = msgTTL;
      return this;
   }

   public int getTransactionBatchSize() {
      return transactionBatchSize;
   }

   public ProducerThread setTransactionBatchSize(int transactionBatchSize) {
      this.transactionBatchSize = transactionBatchSize;
      return this;
   }

   public String getMsgGroupID() {
      return msgGroupID;
   }

   public ProducerThread setMsgGroupID(String msgGroupID) {
      this.msgGroupID = msgGroupID;
      return this;
   }

   public int getTextMessageSize() {
      return textMessageSize;
   }

   public ProducerThread setTextMessageSize(int textMessageSize) {
      this.textMessageSize = textMessageSize;
      return this;
   }

   public int getMessageSize() {
      return messageSize;
   }

   public ProducerThread setMessageSize(int messageSize) {
      this.messageSize = messageSize;
      return this;
   }

   public String getPayloadUrl() {
      return payloadUrl;
   }

   public ProducerThread setPayloadUrl(String payloadUrl) {
      this.payloadUrl = payloadUrl;
      return this;
   }

   public String getMessage() {
      return message;
   }

   public ProducerThread setMessage(String message) {
      this.message = message;
      return this;
   }

   public boolean isVerbose() {
      return verbose;
   }

   public ProducerThread setVerbose(boolean verbose) {
      this.verbose = verbose;
      return this;
   }

   public int getObjectSize() {
      return objectSize;
   }

   public ProducerThread setObjectSize(int objectSize) {
      this.objectSize = objectSize;
      return this;
   }

   public ProducerThread setSentCount(LongAdder sentCount) {
      this.sentCount = sentCount;
      return this;
   }

   public ProducerThread setTarget(long target) {
      this.target = target;
      return this;
   }

   public ProducerThread setRecorder(ValueRecorder recorder) {
      this.recorder = recorder;
      return this;
   }

   public ProducerThread setTimeProvider(LongSupplier timeProvider) {
      this.timeProvider = timeProvider;
      return this;
   }
}
