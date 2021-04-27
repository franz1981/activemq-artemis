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

package org.apache.activemq.artemis.tests.smoke.quorum;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.quorum.DistributedPrimitiveManager;
import org.jboss.logging.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

public class AtomixPluggableQuorumSinglePairTest extends PluggableQuorumSinglePairTest {

   private static final Logger LOGGER = Logger.getLogger(AtomixPluggableQuorumSinglePairTest.class);
   @Rule
   public TemporaryFolder tmpFolder = new TemporaryFolder();
   private DistributedPrimitiveManager witness;
   private Thread asyncStartWitness;
   private CountDownLatch witnessStarted;
   private CountDownLatch witnessRequestStop;

   public AtomixPluggableQuorumSinglePairTest() {
      super("atomix");
   }

   @Override
   @After
   public void after() throws Exception {
      try {
         super.after();
      } finally {
         stopWitness();
      }
   }

   private void stopWitness() {
      if (asyncStartWitness.isInterrupted()) {
         return;
      }
      witnessRequestStop.countDown();
      try {
         asyncStartWitness.join(30_000);
      } catch (InterruptedException e) {
         // no op
      }
      if (asyncStartWitness.isAlive()) {
         asyncStartWitness.interrupt();
      }
   }

   @Before
   @Override
   public void setup() throws Exception {
      super.setup();
      final File witnessFolder = tmpFolder.newFolder("witness");
      LOGGER.infof("Starting witness on %s", witnessFolder);
      final Map<String, String> witnessConfig = new HashMap<>();
      witnessConfig.put("data-dir", witnessFolder.toString());
      witnessConfig.put("id", "witness");
      witnessConfig.put("nodes", "witness@localhost:7778,primary@localhost:7777,backup@localhost:7779");
      witness = DistributedPrimitiveManager.newInstanceOf(ActiveMQDefaultConfiguration.getDefaultDistributedPrimitiveManagerClassName(), witnessConfig);
      final CountDownLatch witnessStarting = new CountDownLatch(1);
      witnessStarted = new CountDownLatch(1);
      witnessRequestStop = new CountDownLatch(1);
      asyncStartWitness = new Thread(() -> {
         witnessStarting.countDown();
         try {
            witness.start();
            witnessStarted.countDown();
            witnessRequestStop.await();
         } catch (Throwable e) {
            LOGGER.warn("Cannot start witness", e);
         } finally {
            witness.stop();
         }
      });
      asyncStartWitness.setDaemon(true);
      asyncStartWitness.setName("start-witness");
      asyncStartWitness.start();
      if (!witnessStarting.await(1, TimeUnit.MINUTES)) {
         LOGGER.warn("Starting witness thread is taking too much time, giving up");
         asyncStartWitness.interrupt();
         throw new IllegalStateException("slow Thread::start");
      }
   }

   @Override
   protected boolean awaitAsyncSetupCompleted(long timeout, TimeUnit unit) throws InterruptedException {
      return witnessStarted.await(timeout, unit);
   }

   @Override
   protected void stopMajority() {
      stopWitness();
   }
}
