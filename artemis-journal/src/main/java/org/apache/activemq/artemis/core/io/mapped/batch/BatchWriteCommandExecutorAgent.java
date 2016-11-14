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

package org.apache.activemq.artemis.core.io.mapped.batch;

import java.nio.ByteBuffer;

import io.netty.util.internal.PlatformDependent;
import org.apache.activemq.artemis.concurrent.ringbuffer.MessageRefConsumer;
import org.apache.activemq.artemis.concurrent.ringbuffer.RefRingBuffer;

final class BatchWriteCommandExecutorAgent implements MessageRefConsumer<WriteRequest> {

   private final RefRingBuffer<WriteRequest> writeRequests;
   private final BatchWriteCommandExecutor commandExecutor;

   BatchWriteCommandExecutorAgent(RefRingBuffer<WriteRequest> writeRequests,
                                  BatchWriteCommandExecutor commandExecutor) {
      this.writeRequests = writeRequests;
      this.commandExecutor = commandExecutor;
   }

   public boolean isClosed() {
      return this.commandExecutor.isClosed();
   }

   public int doWork() {
      if (this.commandExecutor.isClosed()) {
         return 0;
      }
      final int requests = writeRequests.read(this, writeRequests.refCapacity());
      commandExecutor.endOfBatch();
      return requests;
   }

   public void cleanUp() {
      if (this.commandExecutor.isClosed()) {
         //cleanup every pending requests
         while (writeRequests.size() > 0) {
            this.writeRequests.read(this);
         }
         //clean resources
         PlatformDependent.freeDirectBuffer(this.writeRequests.buffer());
      }
   }

   @Override
   public void accept(WriteRequest writeRequest, int msgTypeId, ByteBuffer buffer, int index, int length) {
      try {
         switch (msgTypeId) {
            case BatchWriteCommands.WRITE_REQUEST_MSG_ID:
               this.commandExecutor.onWrite(buffer, index, length, writeRequest.callback);
               break;
            case BatchWriteCommands.WRITE_AND_FLUSH_REQUEST_MSG_ID:
               this.commandExecutor.onWriteAndFlush(buffer, index, length, writeRequest.callback);
               break;
            case BatchWriteCommands.FLUSH_REQUEST_MSG_ID:
               this.commandExecutor.onFlush(writeRequest.callback);
               break;
            case BatchWriteCommands.CLOSE_REQUEST_MSG_ID:
               this.commandExecutor.onClose();
               break;
            case BatchWriteCommands.CHANGE_OBSERVER_REQUEST_MSG_ID:
               this.commandExecutor.onChangeObserver(writeRequest.observer, writeRequest.callback);
               break;
         }
      } finally {
         writeRequest.callback = null;
         writeRequest.observer = null;
      }
   }
}
