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
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import io.atomix.cluster.MemberId;
import io.atomix.cluster.Node;
import io.atomix.cluster.discovery.BootstrapDiscoveryProvider;
import io.atomix.core.Atomix;
import io.atomix.core.AtomixBuilder;
import io.atomix.core.lock.AtomicLock;
import io.atomix.protocols.raft.MultiRaftProtocol;
import io.atomix.protocols.raft.ReadConsistency;
import io.atomix.protocols.raft.partition.RaftPartitionGroup;
import io.atomix.storage.StorageLevel;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

final class AtomixFactory {

   public static Atomix createAtomix(String localMemberId,
                                     Address address,
                                     File dataDirectory,
                                     Map<String, Address> nodes) {
      if (!nodes.containsKey(localMemberId) || !address.equals(nodes.get(localMemberId))) {
         throw new IllegalArgumentException("the local member id should been included in the node list with the given address");
      }
      final AtomixBuilder atomixBuilder = Atomix.builder().withMemberId(localMemberId).withAddress(address);
      atomixBuilder
         .withMembershipProvider(BootstrapDiscoveryProvider.builder()
         .withNodes(
            nodes.entrySet().stream()
               .map(entry-> Node.builder()
                  .withId(entry.getKey())
                  .withAddress(entry.getValue())
                  .build())
               .collect(Collectors.toList())).build());
      // using Profile.consensus(members) is a short-cut of this but it won't left any config choice
      atomixBuilder
         .withManagementGroup(
         RaftPartitionGroup.builder("system")
            .withNumPartitions(1)
            .withMembers(nodes.keySet())
            .withStorageLevel(StorageLevel.DISK)
            .withDataDirectory(new File(dataDirectory, "management"))
            .build())
         .withPartitionGroups(
         RaftPartitionGroup.builder("data")
            .withNumPartitions(1)
            .withMembers(nodes.keySet())
            .withStorageLevel(StorageLevel.DISK)
            .withDataDirectory(new File(dataDirectory, "data"))
            .build());
      return atomixBuilder.build();
   }

   public static AtomicLock createAtomicLock(Atomix atomix, String lockId) {
      return atomix.<MemberId>atomicLockBuilder(lockId)
         .withSerializer(
            Serializer.builder()
               .withRegistrationRequired(false).build())
         .withProtocol(MultiRaftProtocol.builder()
                          .withMaxTimeout(Duration.ofSeconds(10))
                          .withReadConsistency(ReadConsistency.LINEARIZABLE).build())
         .build();
   }

}
