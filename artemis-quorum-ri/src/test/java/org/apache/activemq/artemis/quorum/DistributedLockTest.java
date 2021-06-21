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
package org.apache.activemq.artemis.quorum;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public abstract class DistributedLockTest {

   private final ArrayList<AutoCloseable> closeables = new ArrayList<>();

   @Before
   public void setupEnv() throws Throwable {
   }

   protected abstract void configureManager(Map<String, String> config);

   protected abstract String managerClassName();

   @After
   public void tearDownEnv() throws Throwable {
      closeables.forEach(closeables -> {
         try {
            closeables.close();
         } catch (Throwable t) {
            // silent here
         }
      });
   }

   protected DistributedPrimitiveManager createManagedDistributeManager() {
      return createManagedDistributeManager(stringStringMap -> {
      });
   }

   protected DistributedPrimitiveManager createManagedDistributeManager(Consumer<? super Map<String, String>> defaultConfiguration) {
      try {
         final HashMap<String, String> config = new HashMap<>();
         configureManager(config);
         defaultConfiguration.accept(config);
         final DistributedPrimitiveManager manager = DistributedPrimitiveManager.newInstanceOf(managerClassName(), config);
         closeables.add(manager);
         return manager;
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   @Test
   public void managerReturnsSameLockIfNotClosed() throws ExecutionException, InterruptedException, TimeoutException {
      DistributedPrimitiveManager manager = createManagedDistributeManager();
      manager.start();
      Assert.assertSame(manager.getDistributedLock("a"), manager.getDistributedLock("a"));
   }

   @Test(expected = IllegalStateException.class)
   public void managerCannotGetLockIfNotStarted() throws ExecutionException, InterruptedException, TimeoutException {
      DistributedPrimitiveManager manager = createManagedDistributeManager();
      manager.getDistributedLock("a");
   }

   @Test(expected = NullPointerException.class)
   public void managerCannotGetLockWithNullLockId() throws ExecutionException, InterruptedException, TimeoutException {
      DistributedPrimitiveManager manager = createManagedDistributeManager();
      manager.start();
      manager.getDistributedLock(null);
   }

   @Test
   public void managerStopUnlockLocks() throws ExecutionException, InterruptedException, TimeoutException, UnavailableStateException {
      DistributedPrimitiveManager manager = createManagedDistributeManager();
      manager.start();
      Assert.assertTrue(manager.getDistributedLock("a").tryLock());
      Assert.assertTrue(manager.getDistributedLock("b").tryLock());
      manager.stop();
      manager.start();
      Assert.assertFalse(manager.getDistributedLock("a").isHeldByCaller());
      Assert.assertFalse(manager.getDistributedLock("b").isHeldByCaller());
   }

   @Test
   public void canAcquireReleasedLock() throws ExecutionException, InterruptedException, TimeoutException, UnavailableStateException {
      DistributedPrimitiveManager manager = createManagedDistributeManager();
      manager.start();
      DistributedPrimitiveManager otherManager = createManagedDistributeManager();
      otherManager.start();
      Assert.assertFalse(manager.getDistributedLock("a").version().isPresent());
      Assert.assertTrue(manager.getDistributedLock("a").tryLock());
      final String version = otherManager.getDistributedLock("a").version().get();
      Assert.assertNotNull(version);
      Assert.assertFalse(otherManager.getDistributedLock("a").tryLock());
      manager.getDistributedLock("a").unlock();
      Assert.assertFalse(otherManager.getDistributedLock("a").version().isPresent());
      Assert.assertTrue(otherManager.getDistributedLock("a").tryLock());
      final String newVersion = otherManager.getDistributedLock("a").version().get();
      Assert.assertNotEquals(version, newVersion);
   }

   @Test
   public void acquireAndReleaseLock() throws ExecutionException, InterruptedException, TimeoutException, UnavailableStateException {
      DistributedPrimitiveManager manager = createManagedDistributeManager();
      manager.start();
      DistributedLock lock = manager.getDistributedLock("a");
      Assert.assertFalse(lock.isHeldByCaller());
      Assert.assertTrue(lock.tryLock());
      String version = lock.version().get();
      Assert.assertNotNull(version);
      Assert.assertTrue(lock.isHeldByCaller());
      lock.unlock();
      Assert.assertFalse(lock.isHeldByCaller());
      Assert.assertTrue(lock.tryLock());
      Assert.assertNotEquals(version, lock.version().get());
   }

   @Test(expected = IllegalStateException.class)
   public void cannotAcquireSameLockTwice() throws ExecutionException, InterruptedException, TimeoutException, UnavailableStateException {
      DistributedPrimitiveManager manager = createManagedDistributeManager();
      manager.start();
      DistributedLock lock = manager.getDistributedLock("a");
      Assert.assertTrue(lock.tryLock());
      lock.tryLock();
   }

   @Test
   public void heldLockIsVisibleByDifferentManagers() throws ExecutionException, InterruptedException, TimeoutException, UnavailableStateException {
      DistributedPrimitiveManager ownerManager = createManagedDistributeManager();
      DistributedPrimitiveManager observerManager = createManagedDistributeManager();
      ownerManager.start();
      observerManager.start();
      Assert.assertTrue(ownerManager.getDistributedLock("a").tryLock());
      Assert.assertTrue(ownerManager.getDistributedLock("a").isHeldByCaller());
      Assert.assertFalse(observerManager.getDistributedLock("a").isHeldByCaller());
   }

   @Test
   public void unlockedLockIsVisibleByDifferentManagers() throws ExecutionException, InterruptedException, TimeoutException, UnavailableStateException {
      DistributedPrimitiveManager ownerManager = createManagedDistributeManager();
      DistributedPrimitiveManager observerManager = createManagedDistributeManager();
      ownerManager.start();
      observerManager.start();
      Assert.assertTrue(ownerManager.getDistributedLock("a").tryLock());
      ownerManager.getDistributedLock("a").unlock();
      Assert.assertFalse(observerManager.getDistributedLock("a").isHeldByCaller());
      Assert.assertFalse(ownerManager.getDistributedLock("a").isHeldByCaller());
   }

   @Test
   public void cannotAcquireSameLockFromDifferentManagers() throws ExecutionException, InterruptedException, TimeoutException, UnavailableStateException {
      DistributedPrimitiveManager ownerManager = createManagedDistributeManager();
      DistributedPrimitiveManager notOwnerManager = createManagedDistributeManager();
      ownerManager.start();
      notOwnerManager.start();
      Assert.assertTrue(ownerManager.getDistributedLock("a").tryLock());
      Assert.assertFalse(notOwnerManager.getDistributedLock("a").tryLock());
   }

   @Test
   public void cannotUnlockFromNotOwnerManager() throws ExecutionException, InterruptedException, TimeoutException, UnavailableStateException {
      DistributedPrimitiveManager ownerManager = createManagedDistributeManager();
      DistributedPrimitiveManager notOwnerManager = createManagedDistributeManager();
      ownerManager.start();
      notOwnerManager.start();
      Assert.assertTrue(ownerManager.getDistributedLock("a").tryLock());
      notOwnerManager.getDistributedLock("a").unlock();
      Assert.assertFalse(notOwnerManager.getDistributedLock("a").isHeldByCaller());
      Assert.assertTrue(ownerManager.getDistributedLock("a").isHeldByCaller());
   }

}

