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

package org.apache.activemq.artemis.core.io;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.activemq.artemis.core.io.mapped.MappedSequentialFileFactory;
import org.junit.Assert;
import org.junit.Test;

public class TransferableSequentialFileSanityTest {

   private final int DEFAULT_CAPACITY = 128;
   private static final File TEST_DIR = new File("./");

   public SequentialFileFactory factory(int capacity) {
      return new MappedSequentialFileFactory(TEST_DIR, capacity, false, 0, 0, (code, message, file) -> {
      }).setDatasync(false);
   }

   @Test(expected = IOException.class)
   public void shouldFailCopyToFileWithInsufficientCapacity() throws Exception {
      final SequentialFile source = factory(DEFAULT_CAPACITY).createSequentialFile("source_1.tmp");
      source.close();
      source.getJavaFile().deleteOnExit();
      source.open();
      source.fill(DEFAULT_CAPACITY);
      final SequentialFile destination = factory(DEFAULT_CAPACITY / 2).createSequentialFile("destination_1.tmp");
      destination.getJavaFile().deleteOnExit();
      source.copyTo(destination);
   }

   @Test
   public void shouldCopyToEmptyFile() throws Exception {
      final byte[] expectedValue = new byte[DEFAULT_CAPACITY];
      Arrays.fill(expectedValue, (byte) 1);
      final SequentialFileFactory factory = factory(DEFAULT_CAPACITY);
      final SequentialFile source = factory.createSequentialFile("source_2.tmp");
      source.getJavaFile().deleteOnExit();
      source.open();
      source.writeDirect(ByteBuffer.wrap(expectedValue), false);
      final SequentialFile destination = factory.createSequentialFile("destination_2.tmp");
      destination.getJavaFile().deleteOnExit();
      source.copyTo(destination);
      destination.open();
      final byte[] readRaw = new byte[(int) destination.size()];
      final ByteBuffer readBytes = ByteBuffer.wrap(readRaw);
      destination.read(readBytes);
      Assert.assertArrayEquals(expectedValue, readRaw);
      destination.close();
   }

   @Test
   public void shouldAppendContentIfCopyToNotEmptyFile() throws Exception {
      final byte[] expectedValue = new byte[DEFAULT_CAPACITY];
      Arrays.fill(expectedValue, (byte) 1);
      final SequentialFile source = factory(DEFAULT_CAPACITY).createSequentialFile("source_3.tmp");
      source.getJavaFile().deleteOnExit();
      source.open();
      source.writeDirect(ByteBuffer.wrap(expectedValue), false);
      final SequentialFile destination = factory(expectedValue.length * 2).createSequentialFile("destination_3.tmp");
      destination.getJavaFile().deleteOnExit();
      destination.open();
      destination.fill(expectedValue.length);
      source.copyTo(destination);
      destination.open();
      final byte[] readRaw = new byte[(int) destination.size() / 2];
      final ByteBuffer readBytes = ByteBuffer.wrap(readRaw);
      destination.read(readBytes);
      Assert.assertArrayEquals(new byte[expectedValue.length], readRaw);
      readBytes.clear();
      destination.read(readBytes);
      Assert.assertArrayEquals(expectedValue, readRaw);
      destination.close();
   }

}
