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

package org.apache.activemq.artemis.core.io.mapped;

import org.apache.activemq.artemis.concurrent.ringbuffer.BytesUtils;

final class MovingAverageBytesChunkPretoucher implements MappedPretoucher {

   //must be OS page aligned!
   private static final int MIN_PRETOUCH_BYTES = (int) BytesUtils.align(262144, BytesUtils.PAGE_SIZE);
   private long pretouchedBaseAddress;
   private int lastPosition;
   private int averageMoved;
   private int lastPretouchLimit;

   MovingAverageBytesChunkPretoucher() {
      this.pretouchedBaseAddress = -1L;
      this.lastPosition = 0;
      this.averageMoved = 0;
      this.lastPretouchLimit = 0;
   }

   @Override
   public int pretouch(final MappedFile mappedFile, final int position) {
      assert position > 0;
      final long mappedAddress = mappedFile.address();
      if (mappedAddress != this.pretouchedBaseAddress) {
         this.pretouchedBaseAddress = mappedAddress;

         this.lastPretouchLimit = 0;
         this.lastPosition = position;
         this.averageMoved = BytesUtils.PAGE_SIZE;
      }
      final int moved = position - this.lastPosition;
      if (moved < 0) {
         //rare but could happen...the position is before the last one, pretouch anyway!!!!
         this.lastPretouchLimit = 0;
         this.averageMoved = BytesUtils.PAGE_SIZE;
      }

      this.lastPosition = position;
      //maintain a weighted average of written chunks
      this.averageMoved = moved / 4 + this.averageMoved * 3 / 4;

      final int lastMappedPagePosition = (int) BytesUtils.align(mappedFile.mapped().capacity() - Long.BYTES, BytesUtils.PAGE_SIZE) - BytesUtils.PAGE_SIZE;
      final int writePagePosition = (int) Math.min(BytesUtils.align(position, BytesUtils.PAGE_SIZE), lastMappedPagePosition);
      if (writePagePosition > this.lastPretouchLimit) {
         //pretouch pages in reverse order to exploit LRU policy of the OS page cache
         final int maxPretouchBytes = Math.max((int) BytesUtils.align(this.averageMoved * 4, BytesUtils.PAGE_SIZE), MIN_PRETOUCH_BYTES);
         final int pretouchLimit = Math.min(writePagePosition + maxPretouchBytes, lastMappedPagePosition);
         int pretouchPagePosition = pretouchLimit;
         while (pretouchPagePosition >= writePagePosition) {
            mappedFile.touch(pretouchPagePosition);
            pretouchPagePosition -= BytesUtils.PAGE_SIZE;
         }
         final int pretouchedBytes = pretouchLimit - writePagePosition;
         this.lastPretouchLimit = pretouchLimit;
         return pretouchedBytes;
      } else {
         return 0;
      }

   }
}
