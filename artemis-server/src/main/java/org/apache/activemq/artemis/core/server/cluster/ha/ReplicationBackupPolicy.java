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
package org.apache.activemq.artemis.core.server.cluster.ha;

import java.util.Map;

import org.apache.activemq.artemis.core.config.ha.ReplicationBackupPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.DistributedPrimitiveManagerConfiguration;
import org.apache.activemq.artemis.core.io.IOCriticalErrorListener;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.artemis.core.server.impl.ReplicationBackupActivation;
import org.apache.activemq.artemis.quorum.DistributedPrimitiveManager;

public class ReplicationBackupPolicy implements HAPolicy<ReplicationBackupActivation> {

   private final boolean allowFailback;
   private ReplicationPrimaryPolicy livePolicy;
   private final String groupName;
   private final String clusterName;
   private final int maxSavedReplicatedJournalsSize;
   private final int quorumVoteWait;
   private final int voteRetries;
   private final long voteRetryWait;
   private final long retryReplicationWait;
   private final long initialReplicationSyncTimeout;
   private final DistributedPrimitiveManagerConfiguration distributedManagerConfiguration;

   private ReplicationBackupPolicy(ReplicationBackupPolicyConfiguration configuration, ReplicationPrimaryPolicy livePolicy) {
      this.clusterName = configuration.getClusterName();
      this.maxSavedReplicatedJournalsSize = configuration.getMaxSavedReplicatedJournalsSize();
      this.groupName = configuration.getGroupName();
      this.allowFailback = configuration.isAllowFailBack();
      this.initialReplicationSyncTimeout = configuration.getInitialReplicationSyncTimeout();
      this.voteRetries = configuration.getVoteRetries();
      this.voteRetryWait = configuration.getVoteRetryWait();
      this.retryReplicationWait = configuration.getRetryReplicationWait();
      this.quorumVoteWait = configuration.getQuorumVoteWait();
      this.livePolicy = livePolicy;
      this.distributedManagerConfiguration = configuration.getDistributedManagerConfiguration();
   }

   public static ReplicationBackupPolicy with(ReplicationBackupPolicyConfiguration configuration) {
      return new ReplicationBackupPolicy(configuration, null);
   }

   static ReplicationBackupPolicy with(int quorumVoteWait, int voteRetries, long voteRetryWait, long retryReplicationWait,
                                       String clusterName, String groupName, ReplicationPrimaryPolicy livePolicy,
                                       DistributedPrimitiveManagerConfiguration distributedManagerConfiguration) {
      return new ReplicationBackupPolicy(
         ReplicationBackupPolicyConfiguration.withDefault()
            .setQuorumVoteWait(quorumVoteWait)
            .setVoteRetries(voteRetries)
            .setVoteRetryWait(voteRetryWait)
            .setRetryReplicationWait(retryReplicationWait)
            .setClusterName(clusterName)
            .setGroupName(groupName)
            .setDistributedManagerConfiguration(distributedManagerConfiguration), livePolicy);
   }

   @Override
   public ReplicationBackupActivation createActivation(ActiveMQServerImpl server,
                                                       boolean wasLive,
                                                       Map<String, Object> activationParams,
                                                       IOCriticalErrorListener shutdownOnCriticalIO) throws Exception {
      return new ReplicationBackupActivation(server, wasLive,
                                             DistributedPrimitiveManager.newInstanceOf(
                                                distributedManagerConfiguration.getClassName(),
                                                distributedManagerConfiguration.getProperties()), this);
   }

   @Override
   public boolean isSharedStore() {
      return false;
   }

   @Override
   public boolean isBackup() {
      return true;
   }

   @Override
   public boolean canScaleDown() {
      return false;
   }

   @Override
   public String getScaleDownGroupName() {
      return null;
   }

   @Override
   public String getScaleDownClustername() {
      return null;
   }

   public String getClusterName() {
      return clusterName;
   }

   @Override
   public String getBackupGroupName() {
      return groupName;
   }

   public String getGroupName() {
      return groupName;
   }

   public ReplicationPrimaryPolicy getLivePolicy() {
      if (livePolicy == null) {
         livePolicy = ReplicationPrimaryPolicy.with(initialReplicationSyncTimeout, groupName,
                                                    clusterName, quorumVoteWait, this, allowFailback,
                                                    distributedManagerConfiguration);
      }
      return livePolicy;
   }

   public int getMaxSavedReplicatedJournalsSize() {
      return maxSavedReplicatedJournalsSize;
   }

   public int getVoteRetries() {
      return voteRetries;
   }

   public long getVoteRetryWait() {
      return voteRetryWait;
   }

   public int getQuorumVoteWait() {
      return quorumVoteWait;
   }

   public long getRetryReplicationWait() {
      return retryReplicationWait;
   }

   public long getInitialReplicationSyncTimeout() {
      return initialReplicationSyncTimeout;
   }

   public void setLivePolicy(ReplicationPrimaryPolicy replicatedPolicy) {
      this.livePolicy = replicatedPolicy;
   }

   @Override
   public boolean useQuorumManager() {
      return false;
   }
}
