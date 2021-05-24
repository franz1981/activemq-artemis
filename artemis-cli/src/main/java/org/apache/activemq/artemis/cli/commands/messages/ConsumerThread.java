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
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import java.util.Enumeration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;

import org.HdrHistogram.ValueRecorder;

public class ConsumerThread extends Thread {

   long messageCount = 1000;
   int receiveTimeOut = 3000;
   Destination destination;
   Session session;
   boolean durable;
   boolean breakOnNull = true;
   int sleep;
   int batchSize;
   boolean verbose;
   boolean browse;

   String filter;

   LongAdder totalReceived;
   long received = 0;
   int transactions = 0;
   volatile boolean running = true;
   CountDownLatch finished;
   boolean bytesAsText;
   MessageListener listener;
   ValueRecorder recorder;
   LongSupplier timeProvider;

   public ConsumerThread(Session session, Destination destination, int threadNr) {
      super("Consumer " + destination.toString() + ", thread=" + threadNr);
      this.destination = destination;
      this.session = session;
   }

   public ConsumerThread setTimeProvider(LongSupplier provider) {
      this.timeProvider = provider;
      return this;
   }

   public ConsumerThread setRecorder(ValueRecorder recorder) {
      this.recorder = recorder;
      return this;
   }

   public ConsumerThread setTotalReceived(LongAdder totalReceived) {
      this.totalReceived = totalReceived;
      return this;
   }

   @Override
   public void run() {
      if (browse) {
         browse();
      } else {
         consume();
      }
   }

   private void handle(Message msg, boolean browse) throws JMSException {
      final MessageListener listener = this.listener;
      if (listener != null) {
         listener.onMessage(msg);
         return;
      }
      handleNoListener(msg, browse);
   }

   private void handleNoListener(Message msg, boolean browse) throws JMSException {
      assert listener == null;
      if (browse) {
         handleBrowse(msg);
      } else {
         if (verbose) {
            verboseReceive(msg);
         }
      }
   }

   private void verboseReceive(Message msg) throws JMSException {
      if (bytesAsText && (msg instanceof BytesMessage)) {
         long length = ((BytesMessage) msg).getBodyLength();
         byte[] bytes = new byte[(int) length];
         ((BytesMessage) msg).readBytes(bytes);
         System.out.println("Received a message with " + bytes.length);
      }

      if (msg instanceof TextMessage) {
         String text = ((TextMessage) msg).getText();
         System.out.println("Received text sized at " + text.length());
      }

      if (msg instanceof ObjectMessage) {
         Object obj = ((ObjectMessage) msg).getObject();
         System.out.println("Received object " + obj.toString().length());
      }
   }

   private void handleBrowse(Message msg) throws JMSException {
      if (verbose) {
         System.out.println("..." + msg);
      }
      if (bytesAsText && (msg instanceof BytesMessage)) {
         long length = ((BytesMessage) msg).getBodyLength();
         byte[] bytes = new byte[(int) length];
         ((BytesMessage) msg).readBytes(bytes);
         System.out.println("Message:" + msg);
      }
   }

   public void browse() {
      final LongAdder totalReceived = this.totalReceived;
      QueueBrowser consumer = null;
      String threadName = Thread.currentThread().getName();
      System.out.println(threadName + " trying to browse " + messageCount + " messages");
      try {
         if (filter != null) {
            consumer = session.createBrowser((Queue) destination, filter);
         } else {
            consumer = session.createBrowser((Queue) destination);
         }
         Enumeration<Message> enumBrowse = consumer.getEnumeration();

         while (enumBrowse.hasMoreElements()) {
            Message msg = enumBrowse.nextElement();
            if (msg != null) {
               System.out.println(threadName + " browsing " + (msg instanceof TextMessage ? ((TextMessage) msg).getText() : msg.getJMSMessageID()));
               handle(msg, true);
               received++;
               if (totalReceived != null) {
                  totalReceived.increment();
               }

               if (received >= messageCount) {
                  break;
               }
            } else {
               break;
            }

            if (sleep > 0) {
               Thread.sleep(sleep);
            }

         }

         consumer.close();
      } catch (Exception e) {
         e.printStackTrace();
      } finally {
         if (finished != null) {
            finished.countDown();
         }
         if (consumer != null) {
            System.out.println(threadName + " browsed: " + this.getReceived() + " messages");
            try {
               consumer.close();
            } catch (JMSException e) {
               e.printStackTrace();
            }
         }
      }

      System.out.println(threadName + " Browser thread finished");
   }

   public void consume() {
      MessageConsumer consumer = null;
      String threadName = Thread.currentThread().getName();
      long expectedMessages = Long.MAX_VALUE;
      if (messageCount >= 0) {
         expectedMessages = messageCount;
         System.out.println(threadName + " wait until " + expectedMessages + " messages are consumed");
      }
      try {
         if (durable && destination instanceof Topic) {
            if (filter != null) {
               consumer = session.createDurableSubscriber((Topic) destination, getName(), filter, false);
            } else {
               consumer = session.createDurableSubscriber((Topic) destination, getName());
            }
         } else {
            if (filter != null) {
               consumer = session.createConsumer(destination, filter);
            } else {
               consumer = session.createConsumer(destination);
            }
         }
         long tStart = System.currentTimeMillis();
         final ValueRecorder recorder = this.recorder;
         final LongSupplier timeProvider = this.timeProvider;
         final LongAdder totalReceived = this.totalReceived;
         while (running && received < expectedMessages) {
            Message msg = consumer.receive(receiveTimeOut);
            if (msg != null) {
               if (verbose) {
                  System.out.println(threadName + " Received " + (msg instanceof TextMessage ? ((TextMessage) msg).getText() : msg.getJMSMessageID()));
               }
               if (!browse) {
                  if (recorder != null && timeProvider != null) {
                     final long startUs = msg.getLongProperty("_");
                     final long usLatency = timeProvider.getAsLong() - startUs;
                     if (usLatency < 0) {
                        System.err.println("Timestamp misalignment with sender machine: negative latency!");
                     }
                     recorder.recordValue(usLatency);
                  }
               }
               handle(msg, false);
               received++;
               if (totalReceived != null) {
                  totalReceived.increment();
               }
            } else {
               if (breakOnNull) {
                  break;
               }
            }

            if (session.getTransacted()) {
               if (batchSize > 0 && received > 0 && received % batchSize == 0) {
                  if (verbose) {
                     System.out.println(threadName + " Committing transaction: " + transactions++);
                  }
                  session.commit();
               }
            } else if (session.getAcknowledgeMode() == Session.CLIENT_ACKNOWLEDGE && msg != null) {
               if (batchSize > 0 && received > 0 && received % batchSize == 0) {
                  if (verbose) {
                     System.out.println("Acknowledging last " + batchSize + " messages; messages so far = " + received);
                  }
                  msg.acknowledge();
               }
            }
            if (sleep > 0) {
               Thread.sleep(sleep);
            }

         }

         try {
            session.commit();
         } catch (Throwable ignored) {
         }


         long tEnd = System.currentTimeMillis();
         long elapsed = (tEnd - tStart) / 1000;
         System.out.println(threadName + " Elapsed time in second : " + elapsed + " s");
         System.out.println(threadName + " Elapsed time in milli second : " + (tEnd - tStart) + " milli seconds");

      } catch (Exception e) {
         e.printStackTrace();
      } finally {
         if (finished != null) {
            finished.countDown();
         }
         if (consumer != null) {
            System.out.println(threadName + " Consumed: " + this.getReceived() + " messages");
            try {
               consumer.close();
            } catch (JMSException e) {
               e.printStackTrace();
            }
         }
      }

      System.out.println(threadName + " Consumer thread finished");
   }

   public long getReceived() {
      return received;
   }

   public boolean isDurable() {
      return durable;
   }

   public ConsumerThread setDurable(boolean durable) {
      this.durable = durable;
      return this;
   }

   public ConsumerThread setMessageCount(int messageCount) {
      this.messageCount = messageCount;
      return this;
   }

   public ConsumerThread setBreakOnNull(boolean breakOnNull) {
      this.breakOnNull = breakOnNull;
      return this;
   }

   public int getBatchSize() {
      return batchSize;
   }

   public ConsumerThread setBatchSize(int batchSize) {
      this.batchSize = batchSize;
      return this;
   }

   public long getMessageCount() {
      return messageCount;
   }

   public boolean isBreakOnNull() {
      return breakOnNull;
   }

   public int getReceiveTimeOut() {
      return receiveTimeOut;
   }

   public ConsumerThread setReceiveTimeOut(int receiveTimeOut) {
      this.receiveTimeOut = receiveTimeOut;
      return this;
   }

   public ConsumerThread setRunning(boolean running) {
      this.running = running;
      return this;
   }

   public int getSleep() {
      return sleep;
   }

   public ConsumerThread setSleep(int sleep) {
      this.sleep = sleep;
      return this;
   }

   public CountDownLatch getFinished() {
      return finished;
   }

   public ConsumerThread setFinished(CountDownLatch finished) {
      this.finished = finished;
      return this;
   }

   public boolean isBytesAsText() {
      return bytesAsText;
   }

   public boolean isVerbose() {
      return verbose;
   }

   public ConsumerThread setVerbose(boolean verbose) {
      this.verbose = verbose;
      return this;
   }

   public ConsumerThread setBytesAsText(boolean bytesAsText) {
      this.bytesAsText = bytesAsText;
      return this;
   }

   public String getFilter() {
      return filter;
   }

   public ConsumerThread setFilter(String filter) {
      this.filter = filter;
      return this;
   }

   public boolean isBrowse() {
      return browse;
   }

   public ConsumerThread setBrowse(boolean browse) {
      this.browse = browse;
      return this;
   }

   public ConsumerThread setListener(MessageListener listener) {
      this.listener = listener;
      return this;
   }
}
