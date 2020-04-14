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
package org.apache.activemq.artemis.quorum.atomix;

import java.io.File;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.atomix.utils.net.Address;
import org.apache.activemq.artemis.quorum.DistributedLock;
import org.apache.activemq.artemis.quorum.DistributedLockTest;
import org.apache.activemq.artemis.quorum.DistributedPrimitiveManager;
import org.apache.activemq.artemis.quorum.UnavailableStateException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class AtomixDistributedLockTest extends DistributedLockTest {

   private static final long SESSION_MS = 4_000;
   @Rule
   public TemporaryFolder tmpFolder = new TemporaryFolder();
   private File dataFolder;
   private int managers;
   private static final Address[] NODES;
   private static final String NODES_CONFIG_STRING;
   private DistributedPrimitiveManager witnessManager;
   private Thread witnessStart;

   static {
      NODES = new Address[3];
      NODES[0] = Address.from("localhost:7777");
      NODES[1] = Address.from("localhost:7778");
      NODES[2] = Address.from("localhost:7779");
      NODES_CONFIG_STRING = Stream.of(NODES).map(address -> address + "@" + address).collect(Collectors.joining(","));
   }

   @Override
   @Before
   public void setupEnv() throws Throwable {
      managers = 1;
      dataFolder = tmpFolder.newFolder();
      dataFolder.deleteOnExit();
      final File witnessFolder = tmpFolder.newFolder("witness");
      witnessFolder.deleteOnExit();
      witnessManager = new AtomixDistributedPrimitiveManager(NODES[0].toString(), witnessFolder, AtomixFactory.parseNodes(NODES_CONFIG_STRING, ","));
      final CountDownLatch started = new CountDownLatch(1);
      witnessStart = new Thread(() -> {
         try {
            started.countDown();
            witnessManager.start();
         } catch (ExecutionException | InterruptedException ignore) {
         }
      });
      witnessStart.setDaemon(true);
      witnessStart.start();
      if (!started.await(1, TimeUnit.MINUTES)) {
         throw new IllegalStateException("witness thread not started yet!");
      }
      super.setupEnv();
   }

   @After
   @Override
   public void tearDownEnv() throws Throwable {
      super.tearDownEnv();
      // close witness as last node to help the others to work as expected
      try {
         witnessStart.interrupt();
         witnessStart.join();
      } finally {
         witnessManager.close();
      }
   }

   @Override
   protected void configureManager(Map<String, String> config) {
      if (managers > 2) {
         throw new IllegalStateException("The test has been designed to create just 2 additional node managers");
      }
      config.put("data-dir", dataFolder.toString());
      config.put("nodes", NODES_CONFIG_STRING);
      config.put("folder", NODES[managers].toString());
      config.put("id", NODES[managers].toString());
      config.put("session-ms", Long.toString(SESSION_MS));
      managers++;
   }

   @Override
   protected String managerClassName() {
      return AtomixDistributedPrimitiveManager.class.getName();
   }

   @Test
   public void cannotStartManagerWithDisconnectedServer() throws ExecutionException, InterruptedException {
      final DistributedPrimitiveManager manager = createManagedDistributeManager();
      witnessStart.interrupt();
      witnessStart.join();
      Assert.assertTrue(!witnessManager.isStarted());
      Assert.assertFalse(manager.start(10, TimeUnit.SECONDS));
   }

   @Test
   public void cannotAcquireLockWithDisconnectedServer() throws ExecutionException, InterruptedException, TimeoutException, UnavailableStateException {
      final DistributedPrimitiveManager manager = createManagedDistributeManager();
      manager.start();
      final DistributedLock lock = manager.getDistributedLock("a");
      final CountDownLatch notAvailable = new CountDownLatch(1);
      final DistributedLock.LockListener listener = eventType -> {
         if (eventType == DistributedLock.LockListener.EventType.UNAVAILABLE) {
            notAvailable.countDown();
         }
      };
      lock.addListener(listener);
      witnessStart.join();
      witnessManager.stop();
      Assert.assertTrue(notAvailable.await(30, TimeUnit.SECONDS));
      Assert.assertFalse(lock.tryLock());
   }

   @Test(expected = UnavailableStateException.class)
   public void cannotTryLockWithDisconnectedServer() throws ExecutionException, InterruptedException, TimeoutException, UnavailableStateException {
      final DistributedPrimitiveManager manager = createManagedDistributeManager();
      manager.start();
      final DistributedLock lock = manager.getDistributedLock("a");
      witnessStart.join();
      witnessManager.stop();
      Thread.sleep(SESSION_MS);
      lock.tryLock();
   }

   @Test(expected = UnavailableStateException.class)
   public void cannotCheckLockStatusWithDisconnectedServer() throws ExecutionException, InterruptedException, TimeoutException, UnavailableStateException {
      final DistributedPrimitiveManager manager = createManagedDistributeManager();
      manager.start();
      final DistributedLock lock = manager.getDistributedLock("a");
      Assert.assertTrue(lock.tryLock());
      witnessStart.join();
      witnessManager.stop();
      lock.isHeldByCaller();
   }

   @Test(expected = UnavailableStateException.class)
   public void looseLockAfterServerStop() throws ExecutionException, InterruptedException, TimeoutException, UnavailableStateException {
      final DistributedPrimitiveManager manager = createManagedDistributeManager();
      manager.start();
      final DistributedLock lock = manager.getDistributedLock("a");
      Assert.assertTrue(lock.tryLock());
      Assert.assertTrue(lock.isHeldByCaller());
      final CountDownLatch notAvailable = new CountDownLatch(1);
      final DistributedLock.LockListener listener = eventType -> {
         if (eventType == DistributedLock.LockListener.EventType.UNAVAILABLE) {
            notAvailable.countDown();
         }
      };
      lock.addListener(listener);
      Assert.assertEquals(1, notAvailable.getCount());
      witnessStart.join();
      witnessManager.stop();
      Assert.assertTrue(notAvailable.await(30, TimeUnit.SECONDS));
      lock.isHeldByCaller();
   }

   @Test
   public void canAcquireLockOnServerRestart() throws ExecutionException, InterruptedException, UnavailableStateException, TimeoutException {
      final DistributedPrimitiveManager manager = createManagedDistributeManager();
      manager.start();
      final DistributedLock lock = manager.getDistributedLock("a");
      Assert.assertTrue(lock.tryLock());
      Assert.assertTrue(lock.isHeldByCaller());
      final CountDownLatch notAvailable = new CountDownLatch(1);
      final DistributedLock.LockListener listener = eventType -> {
         if (eventType == DistributedLock.LockListener.EventType.UNAVAILABLE) {
            notAvailable.countDown();
         }
      };
      lock.addListener(listener);
      Assert.assertEquals(1, notAvailable.getCount());
      witnessStart.join();
      witnessManager.stop();
      notAvailable.await();
      manager.stop();
      final CountDownLatch startingWitness = new CountDownLatch(1);
      witnessStart = new Thread(() -> {
         try {
            startingWitness.countDown();
            witnessManager.start();
         } catch (Throwable ignoreMe) {

         }
      });
      witnessStart.setDaemon(true);
      witnessStart.start();
      final DistributedPrimitiveManager otherManager = createManagedDistributeManager();
      Assert.assertTrue(startingWitness.await(30, TimeUnit.SECONDS));
      otherManager.start();
      Thread.sleep(SESSION_MS);
      Assert.assertTrue(otherManager.getDistributedLock("a").tryLock());
   }
}
