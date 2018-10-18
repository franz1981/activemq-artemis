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

package org.apache.activemq.artemis.tests.integration.cluster.distribution;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.core.protocol.openwire.OpenWireProtocolManagerFactory;
import org.apache.activemq.artemis.core.server.cluster.impl.MessageLoadBalancingType;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.junit.Before;
import org.junit.Test;

public class OpenWireAdvisoryTopicClusterTest extends ClusterTestBase {

   private static final boolean persistenceEnabled = false;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      start();
   }

   private void start() throws Exception {
      setupServers();

      setRedistributionDelay(0);
   }

   protected boolean isNetty() {
      return true;
   }

   @Test
   public void testSendReceiveLargeMessage() throws Exception {
      setupCluster(MessageLoadBalancingType.ON_DEMAND);

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      final ConnectionFactory connection1Factory = new ActiveMQConnectionFactory("tcp://localhost:61616");
      final Connection connNode1 = connection1Factory.createConnection();
      connNode1.start();
      final ConnectionFactory connection2Factory = new ActiveMQConnectionFactory("tcp://localhost:61618");
      final Connection connNode3 = connection2Factory.createConnection();
      connNode3.start();
      try {
         final Session sessionNode1 = connNode1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue tempQueue = sessionNode1.createTemporaryQueue();
         for (int i = 0; i < 100000; i++) {
            tempQueue = sessionNode1.createTemporaryQueue();
         }
         System.out.println("ANY IIOBE SHOULD HAPPEN BEFORE THIS POINT");
         System.out.println("CREATED TEMPORARY QUEUES!");
         waitForBindings(0, tempQueue.getQueueName(), 1, 0, true);
         System.out.println("LAST TEMP BINDING CREATED!");
      } finally {
         connNode1.close();
         connNode3.close();
      }
   }

   protected void setupCluster(final MessageLoadBalancingType messageLoadBalancingType) throws Exception {
      setupClusterConnection("cluster", "", messageLoadBalancingType, 1, isNetty(), 0, 1, 2);
      setupClusterConnection("cluster", "", messageLoadBalancingType, 1, isNetty(), 1, 0, 2);
      setupClusterConnection("cluster", "", messageLoadBalancingType, 1, isNetty(), 2, 0, 1);
   }

   protected void setRedistributionDelay(final long delay) {
      AddressSettings as = new AddressSettings().setRedistributionDelay(delay);

      getServer(0).getAddressSettingsRepository().addMatch("", as);
      getServer(1).getAddressSettingsRepository().addMatch("", as);
      getServer(2).getAddressSettingsRepository().addMatch("", as);
   }

   protected void setupServers() throws Exception {
      setupServer(0, persistenceEnabled, isNetty());
      setupServer(1, persistenceEnabled, isNetty());
      setupServer(2, persistenceEnabled, isNetty());
      servers[0].addProtocolManagerFactory(new OpenWireProtocolManagerFactory());
      servers[1].addProtocolManagerFactory(new OpenWireProtocolManagerFactory());
      servers[2].addProtocolManagerFactory(new OpenWireProtocolManagerFactory());
   }

   protected void stopServers() throws Exception {
      closeAllConsumers();

      closeAllSessionFactories();

      closeAllServerLocatorsFactories();

      stopServers(0, 1, 2);

      clearServer(0, 1, 2);

   }

}
