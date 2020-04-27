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
package org.apache.activemq.artemis.cli.commands.quorum;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.activemq.artemis.cli.Artemis;
import org.apache.activemq.artemis.cli.commands.ActionContext;
import org.apache.activemq.artemis.cli.commands.tools.OptionalLocking;
import org.apache.activemq.artemis.quorum.ElectionManager;
import org.apache.activemq.artemis.quorum.atomix.AtomixConfiguration;
import org.apache.activemq.artemis.quorum.atomix.AtomixElectionManager;

@Command(name = "witness", description = "Start a Quorum witness")
public class QuorumWitness extends OptionalLocking {

   @Option(name = "--nodes", description = "A CSV list of key-value pairs in the form \"<node identifier>@<host>:<port>\"")
   public String nodes;

   @Override
   public Object execute(ActionContext context) throws Exception {
      super.execute(context);
      ElectionManager electionManager = new AtomixElectionManager(getFileConfiguration().getQuorumLocation(), AtomixConfiguration.parseNodes(nodes));
      Artemis.printBanner();
      electionManager.start();
      Runtime.getRuntime().addShutdownHook(new Thread("shutdown-hook") {
         @Override
         public void run() {
            electionManager.stop();
         }
      });
      return null;
   }

}
