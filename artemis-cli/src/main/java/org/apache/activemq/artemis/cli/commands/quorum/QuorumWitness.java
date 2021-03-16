/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.cli.commands.quorum;

import java.util.Map;
import java.util.stream.Stream;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.cli.Artemis;
import org.apache.activemq.artemis.cli.commands.AbstractAction;
import org.apache.activemq.artemis.cli.commands.ActionContext;
import org.apache.activemq.artemis.quorum.DistributedPrimitiveManager;

import static java.util.stream.Collectors.toMap;

@Command(name = "witness", description = "Start a Quorum witness")
public class QuorumWitness extends AbstractAction {

   @Option(name = "--parameters", description = "A list of parameters in the form \"<parameter name>=<parameters value>\"")
   public String parameters;
   @Option(name = "--className", description = "full class name of the DistributedPrimitiveManager to be used")
   public String className = ActiveMQDefaultConfiguration.getDefaultDistributedPrimitiveManagerClassName();
   @Option(name = "--separator", description = "chosen separator for parameter list")
   public String separator = ";";

   @Override
   public Object execute(ActionContext context) throws Exception {
      super.execute(context);
      final Map<String, String> propertiesMap = Stream.of(parameters.split(separator))
         .collect(toMap(keyValue -> keyValue.split("=")[0], keyValue -> keyValue.split("=")[1]));
      propertiesMap.putIfAbsent("data-dir", getBrokerConfiguration().getDataLocation().toString());
      final DistributedPrimitiveManager distributedManager = DistributedPrimitiveManager.newInstanceOf(className, propertiesMap);
      Artemis.printBanner();
      distributedManager.start();
      Runtime.getRuntime().addShutdownHook(new Thread("shutdown-hook") {
         @Override
         public void run() {
            distributedManager.stop();
         }
      });
      return null;
   }

}