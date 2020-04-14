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
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import io.atomix.utils.net.Address;
import org.apache.activemq.artemis.quorum.Election;
import org.apache.activemq.artemis.quorum.ElectionManager;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ElectionTest {

   @Rule
   public TemporaryFolder tmpFolder = new TemporaryFolder();
   private static final Map<String, Address> ADDRESSES;
   private static final String[] NODES;

   static {
      ADDRESSES = new HashMap<>(3);
      ADDRESSES.put("node_a", Address.from("localhost:7777"));
      ADDRESSES.put("node_b", Address.from("localhost:7778"));
      ADDRESSES.put("node_c", Address.from("localhost:7779"));
      NODES = new String[3];
      NODES[0] = "node_a";
      NODES[1] = "node_b";
      NODES[2] = "node_c";
   }

   private ElectionManager createElectionManager(String nodeId) throws IOException {
      File tmp = tmpFolder.newFolder(nodeId);
      tmp.deleteOnExit();
      return new AtomixElectionManager(nodeId, ADDRESSES.get(nodeId), tmp, ADDRESSES);
   }

   @Test(expected = TimeoutException.class)
   public void singleNodeCannotStart() throws Throwable {
      ElectionManager manager = createElectionManager(NODES[0]);
      try {
         manager.start().get(2, TimeUnit.SECONDS);
      } finally {
         manager.stop().join();
      }
   }

   @Test
   public void canJoinAndBecameLiveWithQuorum() throws Throwable {
      ElectionManager[] managers = new ElectionManager[2];
      managers[0] = createElectionManager(NODES[0]);
      managers[1] = createElectionManager(NODES[1]);
      CompletableFuture.allOf(managers[0].start(), managers[1].start()).get(2, TimeUnit.MINUTES);
      final String electionId = UUID.randomUUID().toString();
      Election[] elections = Stream.of(managers).map(manager -> manager.joinOrCreateElection(electionId)).toArray(Election[]::new);
      for (Election election : elections) {
         Assert.assertFalse(election.hasWinner());
         Assert.assertFalse(election.isWinner());
      }
      Assert.assertTrue(elections[0].tryWin());
      Assert.assertTrue(elections[0].hasWinner());
      Assert.assertTrue(elections[0].isWinner());
      Assert.assertFalse(elections[1].isWinner());
      Assert.assertTrue(elections[1].hasWinner());
      elections[0].resign();
      for (Election election : elections) {
         Assert.assertFalse(election.hasWinner());
         Assert.assertFalse(election.isWinner());
      }
      for (int i = 0; i < managers.length; i++) {
         managers[i].leaveElection(elections[i]);
      }
      CompletableFuture.allOf(managers[0].stop(), managers[1].stop()).join();
   }

   @Test
   public void expireLiveOnRestartWhileUnavailable() throws Throwable {
      ElectionManager[] managers = new ElectionManager[2];
      managers[0] = createElectionManager(NODES[0]);
      managers[1] = createElectionManager(NODES[1]);
      CompletableFuture.allOf(managers[0].start(), managers[1].start()).get(2, TimeUnit.MINUTES);
      final String electionId = UUID.randomUUID().toString();
      final Election election = managers[0].joinOrCreateElection(electionId);
      final CountDownLatch notAvailable = new CountDownLatch(1);
      final Election.LiveElectionListener listener = (unused, eventType) -> {
         if (eventType == Election.LiveElectionListener.EventType.UNAVAILABLE) {
            notAvailable.countDown();
         }
      };
      election.addListener(listener);
      Assert.assertTrue(election.tryWin());
      // final long winnerVersion = ((AtomixLiveElection)liveElection).version();
      final Election election1 = managers[1].joinOrCreateElection(electionId);
      // Assert.assertTrue(((AtomixLiveElection)election1).isWinner(winnerVersion));
      managers[1].leaveElection(election1);
      managers[1].stop().join();
      long startNotAvailable = System.currentTimeMillis();
      notAvailable.await();
      long elapsedNotAvailable = System.currentTimeMillis() - startNotAvailable;
      System.out.println("Acknowledge not available quorum tooks " + elapsedNotAvailable + " ms");
      try {
         managers[0].leaveElection(election);
         Assert.fail("this should not terminate");
      } catch (Throwable t) {
         Assert.assertNotNull(t);
      }
      managers[0].stop().join();
      CompletableFuture.allOf(managers[0].start(), managers[1].start()).get(2, TimeUnit.MINUTES);
      final Election election1Again = managers[1].joinOrCreateElection(electionId);
      Assert.assertFalse(election1Again.isWinner());
      final Election election0 = managers[0].joinOrCreateElection(electionId);
      // Assert.assertTrue(((AtomixLiveElection)election0).isWinner(winnerVersion));
      System.out.println("started waiting lock to be expired");
      final long started = System.currentTimeMillis();
      while (election0.hasWinner() || election1Again.hasWinner()) {
         TimeUnit.MILLISECONDS.sleep(200);
      }
      final long elapsed = System.currentTimeMillis() - started;
      System.out.println("Acknowledge live isn't there anymore took " + elapsed + " ms");
      // TODO search which parameters can affect this
      managers[0].leaveElection(election0);
      managers[1].leaveElection(election1Again);
      CompletableFuture.allOf(managers[0].stop(), managers[1].stop()).join();
   }

   @Test
   public void leaveElectionCauseLiveResign() throws Throwable {
      ElectionManager[] managers = new ElectionManager[3];
      managers[0] = createElectionManager(NODES[0]);
      managers[1] = createElectionManager(NODES[1]);
      managers[2] = createElectionManager(NODES[2]);
      CompletableFuture.allOf(managers[0].start(), managers[1].start(), managers[2].start()).get(2, TimeUnit.MINUTES);
      final String electionId = UUID.randomUUID().toString();
      final Election election0 = managers[0].joinOrCreateElection(electionId);
      Assert.assertTrue(election0.tryWin());
      managers[0].leaveElection(election0);
      final Election election1 = managers[1].joinOrCreateElection(electionId);
      Assert.assertFalse(election1.hasWinner());
      managers[1].leaveElection(election1);
      {
         final Election election = managers[0].joinOrCreateElection(electionId);
         Assert.assertFalse(election.hasWinner());
         managers[0].leaveElection(election);
      }
      CompletableFuture.allOf(managers[0].stop(), managers[1].stop(), managers[2].stop()).join();
   }

}
