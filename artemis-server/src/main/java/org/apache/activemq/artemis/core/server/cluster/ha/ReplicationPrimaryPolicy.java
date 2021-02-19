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

import org.apache.activemq.artemis.core.config.ha.ReplicationPrimaryPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.DistributedPrimitiveManagerConfiguration;
import org.apache.activemq.artemis.core.io.IOCriticalErrorListener;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.artemis.core.server.impl.ReplicationPrimaryActivation;
import org.apache.activemq.artemis.quorum.DistributedPrimitiveManager;

public class ReplicationPrimaryPolicy implements HAPolicy<ReplicationPrimaryActivation> {

   /*
    * These are only used as the policy when the server is started as a live after a failover
    * */
   private ReplicationBackupPolicy backupPolicy;
   private final int quorumVoteWait;
   private final int voteRetries;
   private final long voteRetryWait;
   private final long retryReplicationWait;
   private final String clusterName;
   private final String groupName;
   private final boolean checkForLiveServer;
   private final long initialReplicationSyncTimeout;
   private final DistributedPrimitiveManagerConfiguration distributedManagerConfiguration;
   /*
    * these are only set by the ReplicaPolicy after failover to decide if the live server can failback, these should not
    * be exposed in configuration.
    **/
   private final boolean allowAutoFailBack;

   private ReplicationPrimaryPolicy(ReplicationPrimaryPolicyConfiguration configuration,
                                    ReplicationBackupPolicy backupPolicy,
                                    boolean allowAutoFailBack) {
      quorumVoteWait = configuration.getQuorumVoteWait();
      voteRetries = configuration.getVoteRetries();
      voteRetryWait = configuration.getVoteRetryWait();
      retryReplicationWait = configuration.getRetryReplicationWait();
      clusterName = configuration.getClusterName();
      groupName = configuration.getGroupName();
      checkForLiveServer = configuration.isCheckForLiveServer();
      initialReplicationSyncTimeout = configuration.getInitialReplicationSyncTimeout();
      distributedManagerConfiguration = configuration.getDistributedManagerConfiguration();
      this.backupPolicy = backupPolicy;
      this.allowAutoFailBack = allowAutoFailBack;
   }

   static ReplicationPrimaryPolicy with(long initialReplicationSyncTimeout,
                                        String groupName,
                                        String clusterName,
                                        int quorumVoteWait,
                                        ReplicationBackupPolicy replicaPolicy,
                                        boolean allowAutoFailback,
                                        DistributedPrimitiveManagerConfiguration distributedManagerConfiguration) {
      return new ReplicationPrimaryPolicy(ReplicationPrimaryPolicyConfiguration.withDefault()
                                             .setCheckForLiveServer(false)
                                             .setInitialReplicationSyncTimeout(initialReplicationSyncTimeout)
                                             .setGroupName(groupName)
                                             .setClusterName(clusterName)
                                             .setQuorumVoteWait(quorumVoteWait)
                                             .setDistributedManagerConfiguration(distributedManagerConfiguration),
                                          replicaPolicy, allowAutoFailback);
   }

   public static ReplicationPrimaryPolicy with(ReplicationPrimaryPolicyConfiguration configuration) {
      return new ReplicationPrimaryPolicy(configuration, null, false);
   }

   public ReplicationBackupPolicy getBackupPolicy() {
      if (backupPolicy == null) {
         backupPolicy = ReplicationBackupPolicy.with(quorumVoteWait, voteRetries, voteRetryWait, retryReplicationWait,
                                                     clusterName, groupName, this, distributedManagerConfiguration);
      }
      return backupPolicy;
   }

   @Override
   public ReplicationPrimaryActivation createActivation(ActiveMQServerImpl server,
                                                        boolean wasLive,
                                                        Map<String, Object> activationParams,
                                                        IOCriticalErrorListener shutdownOnCriticalIO) throws Exception {
      return new ReplicationPrimaryActivation(server,
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
      return false;
   }

   @Override
   public boolean isWaitForActivation() {
      return true;
   }

   @Override
   public boolean canScaleDown() {
      return false;
   }

   @Override
   public String getBackupGroupName() {
      return groupName;
   }

   @Override
   public String getScaleDownGroupName() {
      return null;
   }

   @Override
   public String getScaleDownClustername() {
      return null;
   }

   public boolean isCheckForLiveServer() {
      return checkForLiveServer;
   }

   public boolean isAllowAutoFailBack() {
      return allowAutoFailBack;
   }

   public void setBackupPolicy(ReplicationBackupPolicy backupPolicy) {
      this.backupPolicy = backupPolicy;
   }

   public String getClusterName() {
      return clusterName;
   }

   public long getInitialReplicationSyncTimeout() {
      return initialReplicationSyncTimeout;
   }

   public String getGroupName() {
      return groupName;
   }

   @Override
   public boolean useQuorumManager() {
      return false;
   }
}
