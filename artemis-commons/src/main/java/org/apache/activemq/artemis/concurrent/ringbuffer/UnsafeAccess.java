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

package org.apache.activemq.artemis.concurrent.ringbuffer;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;

final class UnsafeAccess {

   public static final sun.misc.Unsafe UNSAFE;

   static {
      sun.misc.Unsafe instance;
      try {
         final Field field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
         field.setAccessible(true);
         instance = (sun.misc.Unsafe) field.get(null);
      } catch (Exception ignored) {
         // Some platforms, notably Android, might not have a sun.misc.Unsafe
         // implementation with a private `theUnsafe` static instance. In this
         // case we can try and call the default constructor, which proves
         // sufficient for Android usage.
         try {
            Constructor<sun.misc.Unsafe> c = sun.misc.Unsafe.class.getDeclaredConstructor();
            c.setAccessible(true);
            instance = c.newInstance();
         } catch (Exception e) {
            throw new RuntimeException(e);
         }
      }
      UNSAFE = instance;
   }

   private UnsafeAccess() {

   }


}
