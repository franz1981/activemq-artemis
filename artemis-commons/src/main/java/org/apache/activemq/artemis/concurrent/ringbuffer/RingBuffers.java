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

package org.apache.activemq.artemis.concurrent.ringbuffer;

import java.nio.ByteBuffer;
import java.util.function.Supplier;

public final class RingBuffers {

   private RingBuffers() {

   }

   public static int capacity(RingBufferType type, int bytes) {
      switch (type) {

         case SingleProducerSingleConsumer:
            return LamportSpFastFlowScRingBuffer.RingBufferLayout.capacity(bytes);
      }
      throw new AssertionError("case not exists!");
   }

   public static <T> RefRingBuffer<T> withRef(RingBufferType type, ByteBuffer bytes, Supplier<? extends T> refFactory) {
      final FastFlowScRingBuffer ringBuffer = with(type, bytes, MessageLayout.DEFAULT_ALIGNMENT);
      return new RefFastFlowScRingBufferWrapper<>(ringBuffer, refFactory);
   }

   public static <T> RefRingBuffer<T> withRef(RingBufferType type,
                                              ByteBuffer bytes,
                                              Supplier<? extends T> refFactory,
                                              int averageMessageLength) {
      final int messageAlignment = messageAlignment(averageMessageLength);
      final FastFlowScRingBuffer ringBuffer = with(type, bytes, messageAlignment);
      return new RefFastFlowScRingBufferWrapper<>(ringBuffer, refFactory);
   }

   public static RingBuffer with(RingBufferType type, ByteBuffer bytes) {
      final int messageAlignment = MessageLayout.DEFAULT_ALIGNMENT;
      return with(type, bytes, messageAlignment);
   }

   private static FastFlowScRingBuffer with(RingBufferType type, ByteBuffer bytes, int messageAlignment) {
      final FastFlowScRingBuffer ringBuffer;
      switch (type) {

         case SingleProducerSingleConsumer:
            ringBuffer = new LamportSpFastFlowScRingBuffer(bytes, messageAlignment);
            break;
         default:
            throw new AssertionError("unsupported case!");
      }
      return ringBuffer;
   }

   private static int messageAlignment(int messageLength) {
      return BytesUtils.nextPowOf2((int) BytesUtils.align(MessageLayout.HEADER_LENGTH + messageLength, MessageLayout.DEFAULT_ALIGNMENT));
   }

   public static int capacity(RingBufferType type, int messages, int messageLength) {
      final int messageAlignment = messageAlignment(messageLength);
      final int bytes = messages * messageAlignment;
      return capacity(type, bytes);
   }

   public enum RingBufferType {
      SingleProducerSingleConsumer
   }

}
