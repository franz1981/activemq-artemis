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
import org.apache.activemq.artemis.utils.Env;

final class BytesChunckPretoucher implements MappedPretoucher {

   private final int maxPretouchPages;
   private final int maxPretouchBytes;
   private long pretouchedBaseAddress;
   private int lastPretouchLimit;

   BytesChunckPretoucher(int maxBytes) {
      this.pretouchedBaseAddress = -1L;
      this.lastPretouchLimit = 0;
      this.maxPretouchBytes = (int) BytesUtils.align(maxBytes, BytesUtils.PAGE_SIZE);
      this.maxPretouchPages = this.maxPretouchBytes / BytesUtils.PAGE_SIZE;
   }

   @Override
   public int pretouch(final MappedFile mappedFile, final int position) {
      final long mappedAddress = mappedFile.address();
      if (mappedAddress != this.pretouchedBaseAddress) {
         this.lastPretouchLimit = 0;
         this.pretouchedBaseAddress = mappedAddress;
      }
      final int lastMappedPagePosition = (int) BytesUtils.align(mappedFile.mapped().capacity() - Long.BYTES, BytesUtils.PAGE_SIZE) - BytesUtils.PAGE_SIZE;
      final int writePagePosition = (int) Math.min(BytesUtils.align(position, BytesUtils.PAGE_SIZE), lastMappedPagePosition);
      if (writePagePosition > this.lastPretouchLimit) {
         //pretouch pages in reverse order to exploit LRU policy of the OS page cache
         final int pretouchLimit = Math.min(writePagePosition + this.maxPretouchBytes, lastMappedPagePosition);
         int pretouchPagePosition = pretouchLimit;
         while (pretouchPagePosition >= writePagePosition) {
            mappedFile.touch(pretouchPagePosition);
            pretouchPagePosition -= Env.osPageSize();
         }
         final int pretouchedBytes = pretouchLimit - writePagePosition;
         this.lastPretouchLimit = pretouchLimit;
         return pretouchedBytes;
      } else {
         return 0;
      }
   }

}
