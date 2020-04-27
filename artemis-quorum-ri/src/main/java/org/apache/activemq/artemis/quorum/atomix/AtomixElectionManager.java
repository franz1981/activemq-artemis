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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import io.atomix.core.Atomix;
import io.atomix.utils.net.Address;
import org.apache.activemq.artemis.quorum.ElectionManager;
import org.apache.activemq.artemis.quorum.Election;

public final class AtomixElectionManager implements ElectionManager {

   private final File atomixFolder;
   private final Address address;
   private final Map<String, Address> nodes;
   private final String localMemberId;
   private Atomix atomix;
   private AtomixElection liveElection;

   public AtomixElectionManager(File atomixFolder, LinkedHashMap<String, Address> nodes) {
      final Map.Entry<String, Address> localNode = nodes.entrySet().stream().findFirst().get();
      this.atomixFolder = atomixFolder;
      this.nodes = new HashMap<>(nodes);
      this.localMemberId = localNode.getKey();
      this.address = localNode.getValue();
      this.atomix = null;
      this.liveElection = null;
   }

   public AtomixElectionManager(String localMemberId,
                                Address address,
                                File atomixFolder,
                                Map<String, Address> nodes) {
      this.atomixFolder = atomixFolder;
      this.address = address;
      this.nodes = new HashMap<>(nodes);
      this.localMemberId = localMemberId;
      this.atomix = null;
      this.liveElection = null;
   }

   @Override
   public CompletableFuture<?> start() {
      if (this.atomix == null) {
         this.atomix = AtomixFactory.createAtomix(localMemberId, address, atomixFolder, nodes);
         return this.atomix.start();
      } else {
         return CompletableFuture.completedFuture(this.atomix);
      }
   }

   @Override
   public CompletableFuture<?> stop() {
      if (this.atomix != null) {
         // TODO automatically close it?
         if (this.liveElection != null) {
            throw new IllegalStateException("cannot stop this service without leaving the live election first");
         }
         try {
            return this.atomix.stop();
         } finally {
            this.atomix = null;
         }
      } else {
         return CompletableFuture.completedFuture(null);
      }
   }

   @Override
   public Election joinOrCreateElection(String topic) {
      if (this.atomix == null) {
         throw new IllegalStateException("the manager isn't started");
      }
      if (liveElection != null) {
         if (!liveElection.getTopic().equals(topic)) {
            throw new IllegalStateException("There is already a live election: leave it and can create a new one");
         }
         return liveElection;
      }
      liveElection = new AtomixElection(atomix, topic);
      return liveElection;
   }

   @Override
   public void leaveElection(Election election) {
      if (this.atomix == null) {
         throw new IllegalStateException("the manager isn't started");
      }
      if (this.liveElection != election) {
         throw new IllegalStateException("This is not the last created live election");
      }
      AtomixElection atomixLiveElection = this.liveElection;
      this.liveElection = null;
      atomixLiveElection.close();
   }
}
