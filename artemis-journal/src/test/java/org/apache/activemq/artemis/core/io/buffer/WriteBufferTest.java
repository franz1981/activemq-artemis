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

package org.apache.activemq.artemis.core.io.buffer;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.locks.LockSupport;

import io.netty.buffer.Unpooled;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.core.buffers.impl.ChannelBufferWrapper;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.journal.EncodingSupport;
import org.apache.activemq.artemis.core.journal.impl.dataformat.ByteArrayEncoding;
import org.junit.Assert;
import org.junit.Test;

public abstract class WriteBufferTest<T extends WriteBuffer> {

   protected abstract T createWriteBuffer(int maxWrittenBytes);

   @Test
   public void testSaga() {
      final ReusableIOCallback reusableIOCallback = new ReusableIOCallback();
      final TrackingTimedBufferObserver timedBufferObserver = new TrackingTimedBufferObserver(Integer.MAX_VALUE);
      final T writeBuffer = createWriteBuffer(Long.BYTES * 2);
      writeBuffer.start();
      try {
         writeBuffer.setObserver(timedBufferObserver);

         Assert.assertTrue("must be able to write!", writeBuffer.checkSize(Long.BYTES));
         final ActiveMQBuffer activeMQBuffer = new ChannelBufferWrapper(Unpooled.buffer(Long.BYTES, Long.BYTES));
         activeMQBuffer.writerIndex(Long.BYTES);
         reusableIOCallback.reset();
         writeBuffer.addBytes(activeMQBuffer, false, reusableIOCallback);
         while (!reusableIOCallback.isDone()) {
            LockSupport.parkNanos(1L);
         }
         if (reusableIOCallback.errorMessage() != null) {
            throw new IllegalStateException(reusableIOCallback.errorMessage());
         }
         Assert.assertTrue("must be already written that size!", timedBufferObserver.position() == Long.BYTES);
         Assert.assertTrue("must be not sync!", !timedBufferObserver.isSynced());

         Assert.assertTrue("must be able to write!", writeBuffer.checkSize(Long.BYTES));
         final EncodingSupport encodingSupport = new ByteArrayEncoding(new byte[Long.BYTES]);
         reusableIOCallback.reset();
         writeBuffer.addBytes(encodingSupport, true, reusableIOCallback);
         while (!reusableIOCallback.isDone()) {
            LockSupport.parkNanos(1L);
         }
         if (reusableIOCallback.errorMessage() != null) {
            throw new IllegalStateException(reusableIOCallback.errorMessage());
         }
         Assert.assertTrue("must be already written that size!", timedBufferObserver.position() == Long.BYTES * 2);
         Assert.assertTrue("must be sync!", timedBufferObserver.isSynced());
         writeBuffer.flush();
         writeBuffer.setObserver(null);
      } finally {
         writeBuffer.stop();
      }
   }

   private static final class ReusableIOCallback implements IOCallback {

      private static final int WAIT = 0;
      private static final int DONE = 1;

      private static final AtomicIntegerFieldUpdater<ReusableIOCallback> STATE_UPDATER = AtomicIntegerFieldUpdater.newUpdater(ReusableIOCallback.class, "state");

      private volatile int state = WAIT;
      private int errorCode = 0;
      private String errorMessage = null;

      //is safe to call it from every thread!
      public boolean isDone() {
         //ACQUIRE or LoadLoad + LoadStore
         return STATE_UPDATER.get(this) == DONE;
      }

      //is safe to call it after isDone == true
      public int errorCode() {
         return errorCode;
      }

      //is safe to call it after isDone == true
      public String errorMessage() {
         return errorMessage;
      }

      //single writer
      @Override
      public void done() {
         this.errorCode = 0;
         this.errorMessage = null;
         //WriteWrite
         STATE_UPDATER.lazySet(this, DONE);
      }

      //single writer
      @Override
      public void onError(int errorCode, String errorMessage) {
         this.errorCode = errorCode;
         this.errorMessage = errorMessage;
         //WriteWrite
         STATE_UPDATER.lazySet(this, DONE);
      }

      //single writer
      private ReusableIOCallback reset() {
         this.errorCode = 0;
         this.errorMessage = null;
         STATE_UPDATER.lazySet(this, WAIT);
         return this;
      }
   }

}
