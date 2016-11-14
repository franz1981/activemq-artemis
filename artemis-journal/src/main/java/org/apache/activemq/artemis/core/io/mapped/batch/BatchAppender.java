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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.activemq.artemis.core.io.IOCallback;

final class BatchAppender implements BatchWriteCommandExecutor {

   public static final String DISABLE_SMART_BATCH_PROPERTY_NAME = "disable.smart.batch";
   private static final boolean ENABLE_SMART_BATCH = !Boolean.getBoolean(DISABLE_SMART_BATCH_PROPERTY_NAME);

   private final List<IOCallback> callbacks;
   private final long checkpointPeriodNanos;
   private SequentialFileWriter observer;
   private boolean closed;
   private long nextSyncCheckpoint;
   private boolean requiredSync;
   private int pendingFlushes;
   private int estimatedOptimalBatch;

   BatchAppender(long checkpointNanos) {
      this.checkpointPeriodNanos = checkpointNanos;
      this.observer = null;
      this.closed = false;
      this.callbacks = new ArrayList<>();
      this.requiredSync = false;
      this.nextSyncCheckpoint = Long.MAX_VALUE;
      this.pendingFlushes = 0;
      this.estimatedOptimalBatch = Runtime.getRuntime().availableProcessors();
   }

   @Override
   public void onWrite(ByteBuffer buffer, int index, int length, IOCallback callback) {
      if (closed) {
         if (callback != null) {
            callback.onError(-1, "can't write messages after a close!");
         }
      } else {
         if (this.observer == null) {
            if (callback != null) {
               callback.onError(-1, "can't write messages without a TimedBufferObserver!");
            }
         } else {
            flushBatch(buffer, index, length);
            if (callback != null) {
               callbacks.add(callback);
            }
         }
      }
   }

   @Override
   public void onWriteAndFlush(ByteBuffer buffer, int index, int length, IOCallback callback) {
      if (closed) {
         if (callback != null) {
            callback.onError(-1, "can't write messages after a close!");
         }
      } else {
         if (this.observer == null) {
            if (callback != null) {
               callback.onError(-1, "can't write messages without a TimedBufferObserver!");
            }
         } else {
            flushBatch(buffer, index, length);
            this.pendingFlushes++;
            this.requiredSync = true;
            if (callback != null) {
               callbacks.add(callback);
            }
         }
      }
   }

   @Override
   public void onClose() {
      if (!closed) {
         innerEndOfBatch(true);
         this.observer = null;
         this.closed = true;
      }
   }

   @Override
   public void onFlush(IOCallback callback) {
      if (closed) {
         callback.onError(-1, "can't flush messages after a close!");
      } else {
         if (this.observer == null) {
            callback.onError(-1, "can't flush messages without a TimedBufferObserver!");
         } else {
            this.requiredSync = true;
            this.pendingFlushes++;
            this.callbacks.add(callback);
         }
      }
   }

   @Override
   public void onChangeObserver(SequentialFileWriter newObserver, IOCallback callback) {
      if (closed) {
         callback.onError(-1, "can't set a TimedBufferObserver after a close!");
      } else {
         try {
            if (this.observer != null) {
               //it is a premature end of batch!
               innerEndOfBatch(true);
               //alert the observer that no write buffer is active on it!
               this.observer.onDeactivatedTimedBuffer();
            } else {
               //reset everything
               this.nextSyncCheckpoint = System.nanoTime() + this.checkpointPeriodNanos;
               this.pendingFlushes = 0;
               this.requiredSync = false;
               this.callbacks.clear();
            }
         } finally {
            this.observer = newObserver;
            if (this.observer != null) {
               try {
                  callback.done();
               } catch (Exception ex) {
                  callback.onError(-1, ex.getMessage());
               }
            } else {
               callback.done();
            }
         }
      }
   }

   @Override
   public boolean isClosed() {
      return closed;
   }

   @Override
   public void endOfBatch() {
      //only on checkpoint
      innerEndOfBatch(false);
   }

   private void innerEndOfBatch(boolean force) {
      if (!this.closed & this.observer != null) {
         //in mixed workload we could have this call only to close the callbacks
         if (this.requiredSync || !this.callbacks.isEmpty()) {
            final boolean checkpoint = System.nanoTime() > this.nextSyncCheckpoint;
            //the evaluation of the current flush batch and the checkpoint is only when there are required any syncs
            if (force || !this.requiredSync || (this.requiredSync && ((ENABLE_SMART_BATCH && (this.pendingFlushes >= this.estimatedOptimalBatch)) || checkpoint))) {
               try {
                  this.observer.flushBuffer(null, 0, 0, requiredSync, callbacks);
               } finally {
                  //update the estimated optimal batch
                  if (ENABLE_SMART_BATCH) {
                     if (!force) {
                        //on mixed workloads (syncs and not syncs) adjust the estimation only when exists any requires syncs
                        if (this.pendingFlushes > 0) {
                           if (checkpoint) {
                              //anytime the checkpoint is passed, the current collected flushes is the value to aim to.
                              //If this value will be too big (batches too long), we will have another checkpoint that will represent the maximal concurrency
                              //in the given time and will be reevaluated.
                              //If the value estimated will be too short (batches too short) you'll have the rest of the requests
                              //enqueued and there will be a chance that the next evaluations will raise the bar of the estimation (look below)
                              this.estimatedOptimalBatch = this.pendingFlushes;
                           } else {
                              //we are under the latest prediction effect:
                              //evaluate the prediction maximizing the flushes -> this could lead to a checkpoint and to reevaluate the optimal batch at all
                              //the "queuing effect" lead to have increasing optimal batch:
                              //at each failed predication (batches too shorts) the next ones will be bigger because enqueued
                              //until too big and the checkpoint will sign the new target
                              this.estimatedOptimalBatch = Math.max(this.estimatedOptimalBatch, this.pendingFlushes);
                           }
                        }
                     }
                  }
                  //reset the state
                  this.pendingFlushes = 0;
                  this.callbacks.clear();
                  this.requiredSync = false;
                  //uses the same logic as the TimedBuffer
                  this.nextSyncCheckpoint = System.nanoTime() + this.checkpointPeriodNanos;
               }
            }
         }
      }
   }

   private void flushBatch(ByteBuffer buffer, int index, int length) {
      if (!this.closed & this.observer != null) {
         if (length > 0) {
            this.observer.flushBuffer(buffer, index, length, false, Collections.emptyList());
         }
      }
   }
}
