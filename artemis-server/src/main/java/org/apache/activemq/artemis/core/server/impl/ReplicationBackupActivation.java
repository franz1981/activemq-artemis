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
package org.apache.activemq.artemis.core.server.impl;

import javax.annotation.concurrent.GuardedBy;

import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQIllegalStateException;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.protocol.core.Channel;
import org.apache.activemq.artemis.core.replication.ReplicationEndpoint;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.LiveNodeLocator;
import org.apache.activemq.artemis.core.server.NodeManager;;
import org.apache.activemq.artemis.core.server.cluster.ClusterControl;
import org.apache.activemq.artemis.core.server.cluster.ClusterController;
import org.apache.activemq.artemis.core.server.cluster.ha.ReplicationBackupPolicy;
import org.apache.activemq.artemis.quorum.DistributedLock;
import org.apache.activemq.artemis.quorum.DistributedPrimitiveManager;
import org.apache.activemq.artemis.quorum.UnavailableStateException;
import org.jboss.logging.Logger;

import static org.apache.activemq.artemis.core.server.impl.ReplicationObserver.ReplicationFailure;

/**
 * This activation can be used by a primary while trying to fail-back ie {@code failback == true} or
 * by a natural-born backup ie {@code failback == false}.<br>
 */
public final class ReplicationBackupActivation extends Activation {

   private static final Logger LOGGER = Logger.getLogger(ReplicationBackupActivation.class);

   private final ReplicationBackupPolicy policy;
   private final ActiveMQServerImpl activeMQServer;
   private final boolean failback;
   // This field is != null iff this node is a primary during a fail-back ie acting as a backup in order to become live again.
   private final String expectedNodeID;
   @GuardedBy("this")
   private boolean closed;
   private final DistributedPrimitiveManager distributedManager;
   // Used for monitoring purposes
   private volatile ReplicationObserver replicationObserver;
   // Used for testing purposes
   private volatile ReplicationEndpoint replicationEndpoint;
   // Used for testing purposes
   private Consumer<ReplicationEndpoint> onReplicationEndpointCreation;

   public ReplicationBackupActivation(final ActiveMQServerImpl activeMQServer,
                                      final boolean failback,
                                      final DistributedPrimitiveManager distributedManager,
                                      final ReplicationBackupPolicy policy) {
      this.activeMQServer = activeMQServer;
      this.failback = failback;
      if (failback) {
         final SimpleString serverNodeID = activeMQServer.getNodeID();
         if (serverNodeID == null || serverNodeID.isEmpty()) {
            throw new IllegalStateException("A failback activation must be biased around a specific NodeID");
         }
         this.expectedNodeID = serverNodeID.toString();
      } else {
         this.expectedNodeID = null;
      }
      this.distributedManager = distributedManager;
      this.policy = policy;
      this.replicationObserver = null;
      this.replicationEndpoint = null;
   }

   /**
    * This util class exists because {@link LiveNodeLocator} need a {@link LiveNodeLocator.BackupRegistrationListener}
    * to forward backup registration failure events: this is used to switch on/off backup registration event listening
    * on an existing locator.
    */
   private static final class RegistrationFailureForwarder implements LiveNodeLocator.BackupRegistrationListener, AutoCloseable {

      private static final LiveNodeLocator.BackupRegistrationListener NOOP_LISTENER = ignore -> {
      };
      private volatile LiveNodeLocator.BackupRegistrationListener listener = NOOP_LISTENER;

      public RegistrationFailureForwarder to(LiveNodeLocator.BackupRegistrationListener listener) {
         this.listener = listener;
         return this;
      }

      @Override
      public void onBackupRegistrationFailed(boolean alreadyReplicating) {
         listener.onBackupRegistrationFailed(alreadyReplicating);
      }

      @Override
      public void close() {
         listener = NOOP_LISTENER;
      }
   }

   @Override
   public void run() {
      synchronized (this) {
         if (closed) {
            return;
         }
      }
      try {
         try {
            // best effort to add an additional "witness" node in case of quorum using distributed consensus
            distributedManager.start(policy.getQuorumVoteWait(), TimeUnit.SECONDS);
         } catch (ExecutionException ignore) {
            LOGGER.debug("Failed to start distributed primitive manager", ignore);
         }
         // Stop the previous node manager and create a new one with NodeManager::replicatedBackup == true:
         // NodeManager::start skip setup lock file with NodeID, until NodeManager::stopBackup is called.
         activeMQServer.resetNodeManager();
         activeMQServer.getNodeManager().stop();
         // A primary need to preserve NodeID across runs
         activeMQServer.moveServerData(policy.getMaxSavedReplicatedJournalsSize(), failback);
         activeMQServer.getNodeManager().start();
         if (!activeMQServer.initialisePart1(false)) {
            return;
         }
         synchronized (this) {
            if (closed)
               return;
         }
         final ClusterController clusterController = activeMQServer.getClusterManager().getClusterController();
         clusterController.awaitConnectionToReplicationCluster();
         activeMQServer.getBackupManager().start();
         ActiveMQServerLogger.LOGGER.backupServerStarted(activeMQServer.getVersion().getFullVersion(),
                                                         activeMQServer.getNodeManager().getNodeId());
         activeMQServer.setState(ActiveMQServerImpl.SERVER_STATE.STARTED);
         final DistributedLock liveLock = replicateAndFailover(clusterController);
         if (liveLock == null) {
            return;
         }
         startAsLive(liveLock);
      } catch (Exception e) {
         if ((e instanceof InterruptedException || e instanceof IllegalStateException) && !activeMQServer.isStarted()) {
            // do not log these errors if the server is being stopped.
            return;
         }
         ActiveMQServerLogger.LOGGER.initializationError(e);
      }
   }

   private void startAsLive(final DistributedLock liveLock) throws Exception {
      policy.getLivePolicy().setBackupPolicy(policy);
      activeMQServer.setHAPolicy(policy.getLivePolicy());

      synchronized (activeMQServer) {
         if (!activeMQServer.isStarted()) {
            liveLock.close();
            return;
         }
         ActiveMQServerLogger.LOGGER.becomingLive(activeMQServer);
         // stopBackup is going to write the NodeID previously set on the NodeManager,
         // because activeMQServer.resetNodeManager() has created a NodeManager with replicatedBackup == true.
         activeMQServer.getNodeManager().stopBackup();
         activeMQServer.getStorageManager().start();
         activeMQServer.getBackupManager().activated();
         // IMPORTANT:
         // we're setting this activation JUST because it would allow the server to use its
         // getActivationChannelHandler to handle replication
         final ReplicationPrimaryActivation primaryActivation = new ReplicationPrimaryActivation(activeMQServer, distributedManager, policy.getLivePolicy());
         liveLock.addListener(primaryActivation);
         activeMQServer.setActivation(primaryActivation);
         activeMQServer.initialisePart2(false);
         // calling primaryActivation.stateChanged !isHelByCaller is necessary in case the lock was unavailable
         // before liveLock.addListener: just throwing an exception won't stop the broker.
         final boolean stillLive;
         try {
            stillLive = liveLock.isHeldByCaller();
         } catch (UnavailableStateException e) {
            LOGGER.warn(e);
            primaryActivation.stateChanged(DistributedLock.LockListener.EventType.UNAVAILABLE);
            throw new ActiveMQIllegalStateException("This server cannot check its role as a live: activation is failed");
         }
         if (!stillLive) {
            primaryActivation.stateChanged(DistributedLock.LockListener.EventType.UNAVAILABLE);
            throw new ActiveMQIllegalStateException("This server is not live anymore: activation is failed");
         }
         if (activeMQServer.getIdentity() != null) {
            ActiveMQServerLogger.LOGGER.serverIsLive(activeMQServer.getIdentity());
         } else {
            ActiveMQServerLogger.LOGGER.serverIsLive();
         }
         activeMQServer.completeActivation(true);
      }
   }

   private LiveNodeLocator createLiveNodeLocator(final LiveNodeLocator.BackupRegistrationListener registrationListener) {
      if (expectedNodeID != null) {
         assert failback;
         return new NamedLiveNodeIdLocatorForReplication(expectedNodeID, registrationListener, policy.getRetryReplicationWait());
      }
      return policy.getGroupName() == null ?
         new AnyLiveNodeLocatorForReplication(registrationListener, activeMQServer, policy.getRetryReplicationWait()) :
         new NamedLiveNodeLocatorForReplication(policy.getGroupName(), registrationListener, policy.getRetryReplicationWait());
   }

   private DistributedLock replicateAndFailover(final ClusterController clusterController) throws ActiveMQException, InterruptedException {
      final RegistrationFailureForwarder registrationFailureForwarder = new RegistrationFailureForwarder();
      // node locator isn't stateless and contains a live-list of candidate nodes to connect too, hence
      // it MUST be reused for each replicateLive attempt
      final LiveNodeLocator nodeLocator = createLiveNodeLocator(registrationFailureForwarder);
      clusterController.addClusterTopologyListenerForReplication(nodeLocator);
      try {
         while (true) {
            synchronized (this) {
               if (closed) {
                  return null;
               }
            }
            final ReplicationFailure failure = replicateLive(clusterController, nodeLocator, registrationFailureForwarder);
            if (failure == null) {
               Thread.sleep(clusterController.getRetryIntervalForReplicatedCluster());
               continue;
            }
            if (!activeMQServer.isStarted()) {
               return null;
            }
            LOGGER.debugf("ReplicationFailure = %s", failure);
            switch (failure) {
               case VoluntaryFailOver:
               case NonVoluntaryFailover:
                  final DistributedLock liveLock = tryAcquireLiveLock();
                  if (liveLock != null) {
                     return liveLock;
                  }
                  if (!failback) {
                     ActiveMQServerLogger.LOGGER.restartingAsBackupBasedOnQuorumVoteResults();
                  }
                  asyncRestartServer(activeMQServer, true);
                  return null;
               case RegistrationError:
                  asyncRestartServer(activeMQServer, false);
                  return null;
               case AlreadyReplicating:
                  // can just retry here, data should be clean and nodeLocator
                  // should remove the live node that has answered this
                  continue;
               case ClosedObserver:
                  return null;
               case BackupNotInSync:
                  asyncRestartServer(activeMQServer, true);
                  return null;
               case WrongNodeId:
                  asyncRestartServer(activeMQServer, true);
                  return null;
               default:
                  throw new AssertionError("Unsupported failure " + failure);
            }
         }
      } finally {
         silentExecution("Errored on cluster topology listener for replication cleanup", () -> clusterController.removeClusterTopologyListenerForReplication(nodeLocator));
      }
   }

   private DistributedLock tryAcquireLiveLock() throws InterruptedException {
      assert activeMQServer.getNodeManager().getNodeId() != null;
      final String liveID = activeMQServer.getNodeManager().getNodeId().toString();
      final int voteRetries = policy.getVoteRetries();
      final long maxAttempts = voteRetries >= 0 ? (voteRetries + 1) : -1;
      final long quorumVoteWaitMs = TimeUnit.SECONDS.toMillis(policy.getQuorumVoteWait());
      final long voteRetryWait = policy.getVoteRetryWait();
      DistributedLock liveLock = null;
      for (long attempt = 0; maxAttempts >= 0 ? (attempt < maxAttempts) : true; attempt++) {
         if (liveLock == null) {
            liveLock = getLock(distributedManager, liveID, quorumVoteWaitMs);
         }
         if (liveLock != null) {
            try {
               if (liveLock.tryLock(quorumVoteWaitMs, TimeUnit.MILLISECONDS)) {
                  LOGGER.debugf("%s live lock acquired after %d attempts.", liveID, (attempt + 1));
                  return liveLock;
               }
            } catch (UnavailableStateException e) {
               LOGGER.warnf(e, "Failed to acquire live lock %s", liveID);
               distributedManager.stop();
               liveLock = null;
            }
         }
         final boolean lastAttempt = maxAttempts >= 0 ? (attempt == (maxAttempts - 1)) : false;
         if (!lastAttempt) {
            TimeUnit.MILLISECONDS.sleep(voteRetryWait);
         }
      }
      distributedManager.stop();
      return null;
   }

   private DistributedLock getLock(final DistributedPrimitiveManager manager,
                                   final String lockId,
                                   final long startDistributedManagerTimeoutMillis) throws InterruptedException {
      try {
         if (manager.start(startDistributedManagerTimeoutMillis, TimeUnit.MILLISECONDS)) {
            try {
               return manager.getDistributedLock(lockId);
            } catch (ExecutionException e) {
               LOGGER.warnf(e, "Failed to get live lock %s", lockId);
               distributedManager.stop();
               return null;
            } catch (TimeoutException e) {
               LOGGER.warnf(e, "Failed to get live lock %s in time", lockId);
               return null;
            }
         }
         return null;
      } catch (ExecutionException e) {
         LOGGER.warnf(e, "Errored while starting distribute manager");
         return null;
      }
   }

   private ReplicationObserver replicationObserver() {
      if (failback) {
         return ReplicationObserver.failbackObserver(activeMQServer.getNodeManager(), activeMQServer.getBackupManager(), activeMQServer.getScheduledPool(), expectedNodeID);
      }
      return ReplicationObserver.failoverObserver(activeMQServer.getNodeManager(), activeMQServer.getBackupManager(), activeMQServer.getScheduledPool());
   }

   private ReplicationFailure replicateLive(final ClusterController clusterController,
                                            final LiveNodeLocator liveLocator,
                                            final RegistrationFailureForwarder registrationFailureForwarder) throws ActiveMQException {
      try (ReplicationObserver replicationObserver = replicationObserver();
           RegistrationFailureForwarder ignored = registrationFailureForwarder.to(replicationObserver)) {
         this.replicationObserver = replicationObserver;
         clusterController.addClusterTopologyListener(replicationObserver);
         // ReplicationError notifies backup registration failures to live locator -> forwarder -> observer
         final ReplicationError replicationError = new ReplicationError(liveLocator);
         clusterController.addIncomingInterceptorForReplication(replicationError);
         try {
            final ClusterControl liveControl = tryLocateAndConnectToLive(liveLocator, clusterController);
            if (liveControl == null) {
               return null;
            }
            try {
               final ReplicationEndpoint replicationEndpoint = tryAuthorizeAndAsyncRegisterAsBackupToLive(liveControl, replicationObserver);
               if (replicationEndpoint == null) {
                  return ReplicationFailure.RegistrationError;
               }
               this.replicationEndpoint = replicationEndpoint;
               assert replicationEndpoint != null;
               try {
                  return replicationObserver.awaitReplicationFailure();
               } finally {
                  this.replicationEndpoint = null;
                  ActiveMQServerImpl.stopComponent(replicationEndpoint);
                  closeChannelOf(replicationEndpoint);
               }
            } finally {
               silentExecution("Errored on live control close", liveControl::close);
            }
         } finally {
            silentExecution("Errored on cluster topology listener cleanup", () -> clusterController.removeClusterTopologyListener(replicationObserver));
            silentExecution("Errored while removing incoming interceptor for replication", () -> clusterController.removeIncomingInterceptorForReplication(replicationError));
         }
      } finally {
         this.replicationObserver = null;
      }
   }

   private static void silentExecution(String debugErrorMessage, Runnable task) {
      try {
         task.run();
      } catch (Throwable ignore) {
         LOGGER.debug(debugErrorMessage, ignore);
      }
   }

   private static void closeChannelOf(final ReplicationEndpoint replicationEndpoint) {
      if (replicationEndpoint == null) {
         return;
      }
      if (replicationEndpoint.getChannel() != null) {
         silentExecution("Errored while closing replication endpoint channel", () -> replicationEndpoint.getChannel().close());
         replicationEndpoint.setChannel(null);
      }
   }

   private void asyncRestartServer(final ActiveMQServer server, boolean restart) {
      new Thread(() -> {
         if (server.getState() != ActiveMQServer.SERVER_STATE.STOPPED && server.getState() != ActiveMQServer.SERVER_STATE.STOPPING) {
            try {
               server.stop(!restart);
               if (restart) {
                  if (failback) {
                     policy.getLivePolicy().setBackupPolicy(policy);
                     activeMQServer.setHAPolicy(policy.getLivePolicy());
                     // don't use the existing data, because are garbage: clean start
                     activeMQServer.moveServerData(policy.getMaxSavedReplicatedJournalsSize(), true);
                  }
                  server.start();
               }
            } catch (Exception e) {
               if (restart) {
                  ActiveMQServerLogger.LOGGER.errorRestartingBackupServer(e, server);
               } else {
                  ActiveMQServerLogger.LOGGER.errorStoppingServer(e);
               }
            }
         }
      }).start();
   }

   private ClusterControl tryLocateAndConnectToLive(final LiveNodeLocator liveLocator,
                                                    final ClusterController clusterController) throws ActiveMQException {
      liveLocator.locateNode();
      final Pair<TransportConfiguration, TransportConfiguration> possibleLive = liveLocator.getLiveConfiguration();
      final String nodeID = liveLocator.getNodeID();
      if (nodeID == null) {
         throw new RuntimeException("Could not establish the connection with any live");
      }
      if (!failback) {
         assert expectedNodeID == null;
         activeMQServer.getNodeManager().setNodeID(nodeID);
      } else {
         assert expectedNodeID.equals(nodeID);
      }
      if (possibleLive == null) {
         return null;
      }
      final ClusterControl liveControl = tryConnectToNodeInReplicatedCluster(clusterController, possibleLive.getA());
      if (liveControl != null) {
         return liveControl;
      }
      return tryConnectToNodeInReplicatedCluster(clusterController, possibleLive.getB());
   }

   private static ClusterControl tryConnectToNodeInReplicatedCluster(final ClusterController clusterController,
                                                                     final TransportConfiguration tc) {
      try {
         if (tc != null) {
            return clusterController.connectToNodeInReplicatedCluster(tc);
         }
      } catch (Exception e) {
         LOGGER.debug(e.getMessage(), e);
      }
      return null;
   }

   @Override
   public void close(final boolean permanently, final boolean restarting) throws Exception {
      synchronized (this) {
         closed = true;
         final ReplicationObserver replicationObserver = this.replicationObserver;
         if (replicationObserver != null) {
            replicationObserver.close();
         }
      }
      distributedManager.stop();
      //we have to check as the server policy may have changed
      if (activeMQServer.getHAPolicy().isBackup()) {
         // To avoid a NPE cause by the stop
         final NodeManager nodeManager = activeMQServer.getNodeManager();

         activeMQServer.interruptActivationThread(nodeManager);

         if (nodeManager != null) {
            nodeManager.stopBackup();
         }
      }
   }

   @Override
   public void preStorageClose() throws Exception {
      // TODO replication endpoint close?
   }

   private ReplicationEndpoint tryAuthorizeAndAsyncRegisterAsBackupToLive(final ClusterControl liveControl,
                                                                          final ReplicationObserver liveObserver) {
      ReplicationEndpoint replicationEndpoint = null;
      try {
         liveControl.getSessionFactory().setReconnectAttempts(1);
         liveObserver.listenConnectionFailuresOf(liveControl.getSessionFactory());
         liveControl.authorize();
         replicationEndpoint = new ReplicationEndpoint(activeMQServer, failback, liveObserver);
         final Consumer<ReplicationEndpoint> onReplicationEndpointCreation = this.onReplicationEndpointCreation;
         if (onReplicationEndpointCreation != null) {
            onReplicationEndpointCreation.accept(replicationEndpoint);
         }
         replicationEndpoint.setExecutor(activeMQServer.getExecutorFactory().getExecutor());
         connectToReplicationEndpoint(liveControl, replicationEndpoint);
         replicationEndpoint.start();
         liveControl.announceReplicatingBackupToLive(failback, policy.getClusterName());
         return replicationEndpoint;
      } catch (Exception e) {
         ActiveMQServerLogger.LOGGER.replicationStartProblem(e);
         ActiveMQServerImpl.stopComponent(replicationEndpoint);
         closeChannelOf(replicationEndpoint);
         return null;
      }
   }

   private static boolean connectToReplicationEndpoint(final ClusterControl liveControl,
                                                final ReplicationEndpoint replicationEndpoint) {
      final Channel replicationChannel = liveControl.createReplicationChannel();
      replicationChannel.setHandler(replicationEndpoint);
      replicationEndpoint.setChannel(replicationChannel);
      return true;
   }

   @Override
   public boolean isReplicaSync() {
      // NOTE: this method is just for monitoring purposes, not suitable to perform logic!
      // During a failover this backup won't have any active liveObserver and will report `false`!!
      final ReplicationObserver liveObserver = this.replicationObserver;
      if (liveObserver == null) {
         return false;
      }
      return liveObserver.isBackupUpToDate();
   }

   public ReplicationEndpoint getReplicationEndpoint() {
      return replicationEndpoint;
   }

   /**
    * This must be used just for testing purposes.
    */
   public void spyReplicationEndpointCreation(Consumer<ReplicationEndpoint> onReplicationEndpointCreation) {
      Objects.requireNonNull(onReplicationEndpointCreation);
      this.onReplicationEndpointCreation = onReplicationEndpointCreation;
   }
}
