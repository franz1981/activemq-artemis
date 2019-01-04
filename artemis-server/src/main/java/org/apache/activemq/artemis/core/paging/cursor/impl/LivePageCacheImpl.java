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
package org.apache.activemq.artemis.core.paging.cursor.impl;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.apache.activemq.artemis.core.paging.PagedMessage;
import org.apache.activemq.artemis.core.paging.cursor.LivePageCache;
import org.apache.activemq.artemis.core.paging.impl.Page;
import org.apache.activemq.artemis.core.server.LargeServerMessage;
import org.jboss.logging.Logger;

/**
 * This is the same as PageCache, however this is for the page that's being currently written.
 */
public final class LivePageCacheImpl implements LivePageCache {

   private static final Logger logger = Logger.getLogger(LivePageCacheImpl.class);

   //IMPORTANT: to enable some nice optimization on / and %, chunck size MUST BE a power of 2
   private static final int chunkSize = 32;

   private static final int chunkMask = chunkSize - 1;

   private static final int chunkSizeLog2 = Integer.numberOfTrailingZeros(chunkSize);

   private static final long RESIZING = -1;

   private AtomicReferenceArray<Object> consumerBuffer = null;

   private AtomicReferenceArray<Object> producerBuffer = null;

   private final AtomicLong producerIndex = new AtomicLong(0);

   private final AtomicLong lastProducerIndex = new AtomicLong(0);

   private final Page page;

   private volatile boolean isLive = true;

   public LivePageCacheImpl(final Page page) {
      this.page = page;
   }

   @Override
   public long getPageId() {
      return page.getPageId();
   }

   @Override
   public int getNumberOfMessages() {
      final AtomicLong producerIndex = this.producerIndex;
      while (true) {
         final long size = producerIndex.get();
         if (size == RESIZING) {
            Thread.yield();
            continue;
         }
         return (int) Math.min(size, Integer.MAX_VALUE);
      }
   }

   @Override
   public void setMessages(PagedMessage[] messages) {
      // This method shouldn't be called on liveCache, but we will provide the implementation for it anyway
      for (PagedMessage msg : messages) {
         addLiveMessage(msg);
      }
   }

   @Override
   public PagedMessage getMessage(int messageNumber) {
      //it allow to perform less cache invalidations vs producerIndex if there are bursts of appends
      final long size = lastProducerIndex.get();
      if (size == 0 || messageNumber >= size) {
         if (!weakUpdateStaleSize(size, messageNumber)) {
            return null;
         }
      }
      //fast division by a power of 2
      final int jumps = messageNumber >> chunkSizeLog2;
      AtomicReferenceArray<Object> buffer = consumerBuffer;
      for (int i = 0; i < jumps; i++) {
         //the next chunk is always set if we stay below a read producerIndex value
         buffer = (AtomicReferenceArray<Object>) buffer.get(chunkSize);
      }
      final int offset = messageNumber & chunkMask;
      //NOTE: producerIndex is being updated before setting a new value ie on consumer side need to spin until a not null value is set
      Object msg;
      while ((msg = buffer.get(offset)) == null) {
         Thread.yield();
      }
      return (PagedMessage) msg;
   }

   private boolean weakUpdateStaleSize(long size, int messageNumber) {
      final AtomicLong producerIndex = this.producerIndex;
      long currentSize;
      while ((currentSize = producerIndex.get()) == RESIZING) {
         Thread.yield();
      }
      //it is really empty or a message over the current size?
      if (currentSize == 0 || messageNumber >= currentSize) {
         return false;
      }
      assert currentSize > size;
      //publish it for others consumers
      lastProducerIndex.lazySet(currentSize);
      return true;
   }

   @Override
   public boolean isLive() {
      return isLive;
   }

   @Override
   public void addLiveMessage(PagedMessage message) {
      if (message.getMessage().isLargeMessage()) {
         ((LargeServerMessage) message.getMessage()).incrementDelayDeletionCount();
      }
      final AtomicLong producerIndex = this.producerIndex;
      while (true) {
         final long pIndex = producerIndex.get();
         if (pIndex != RESIZING) {
            //load acquire the current producer buffer
            final AtomicReferenceArray<Object> producerBuffer = this.producerBuffer;
            final int pOffset = (int) (pIndex & chunkMask);
            //only the first message to a chunk can attempt to resize
            if (pOffset == 0) {
               if (appendChunkAndMessage(producerBuffer, pIndex, message)) {
                  return;
               }
            } else if (producerIndex.compareAndSet(pIndex, pIndex + 1)) {
               //this.producerBuffer is the correct buffer to append a message: it is guarded by the producerIndex logic
               //NOTE: producerIndex is being updated before setting a new value
               producerBuffer.lazySet(pOffset, message);
               return;
            }
         }
         Thread.yield();
      }
   }

   private boolean appendChunkAndMessage(AtomicReferenceArray<Object> producerBuffer,
                                         long pIndex,
                                         PagedMessage message) {
      final AtomicLong producerIndex = this.producerIndex;
      if (!producerIndex.compareAndSet(pIndex, RESIZING)) {
         return false;
      }
      final AtomicReferenceArray<Object> newChunk;
      try {
         newChunk = new AtomicReferenceArray<>(chunkSize + 1);
      } catch (OutOfMemoryError oom) {
         //unblock producerIndex without updating it
         producerIndex.lazySet(pIndex);
         throw oom;
      }
      //adding the message to it
      newChunk.lazySet(0, message);
      //linking it to the old one, if any
      if (producerBuffer != null) {
         producerBuffer.lazySet(chunkSize, newChunk);
      } else {
         //it's first one
         this.consumerBuffer = newChunk;
      }
      //making it the current produced one
      this.producerBuffer = newChunk;
      //store release any previous write and "unblock" anyone waiting resizing to finish
      producerIndex.lazySet(pIndex + 1);
      return true;
   }

   @Override
   public void close() {
      logger.tracef("Closing %s", this);
      this.isLive = false;

   }

   private static PagedMessage[] EMPTY_MSG = null;

   private static PagedMessage[] noMessages() {
      //it is a benign race: no need strong initializations here
      PagedMessage[] empty = EMPTY_MSG;
      if (empty != null) {
         return empty;
      } else {
         empty = new PagedMessage[0];
         EMPTY_MSG = empty;
      }
      return empty;
   }

   @Override
   public PagedMessage[] getMessages() {
      final AtomicLong producerIndex = this.producerIndex;
      long currentSize;
      while ((currentSize = producerIndex.get()) == RESIZING) {
         Thread.yield();
      }
      if (currentSize == 0) {
         return noMessages();
      }
      if (currentSize > Integer.MAX_VALUE) {
         throw new IllegalStateException("can't return PagedMessage[] with more then " + Integer.MAX_VALUE + " elements, numberOfMessages = " + producerIndex);
      }
      final int size = (int) currentSize;
      final PagedMessage[] messages = new PagedMessage[size];
      //fast division by a power of 2
      final int jumps = size >> chunkSizeLog2;
      AtomicReferenceArray<Object> buffer = consumerBuffer;
      int messageNumber = 0;
      for (int i = 0; i < jumps; i++) {
         drainMessages(buffer, messages, messageNumber, chunkSize);
         messageNumber += chunkSize;
         //the next chunk is always set if we stay below a past size/producerIndex value
         buffer = (AtomicReferenceArray<Object>) buffer.get(chunkSize);
      }
      final int remaining = (size & chunkMask);
      drainMessages(buffer, messages, messageNumber, remaining);
      return messages;
   }

   private static void drainMessages(AtomicReferenceArray<Object> buffer,
                                     PagedMessage[] messages,
                                     int messageNumber,
                                     int length) {
      for (int j = 0; j < length; j++) {
         Object msg;
         while ((msg = buffer.get(j)) == null) {
            Thread.yield();
         }
         assert msg != null;
         messages[messageNumber] = (PagedMessage) msg;
         messageNumber++;
      }
   }

   @Override
   public String toString() {
      return "LivePacheCacheImpl::page=" + page.getPageId() + " number of messages=" + getNumberOfMessages() + " isLive = " + isLive;
   }
}
