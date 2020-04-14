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

import java.util.LinkedHashMap;

import io.atomix.utils.net.Address;

public class AtomixConfiguration {

   public static LinkedHashMap<String, Address> parseNodes(String nodes) {
      return parseNodes(nodes, ',', '@');
   }

   public static LinkedHashMap<String, Address> parseNodes(String nodes,
                                                           char nodesSeparator,
                                                           char nodeAddressSeparator) {
      LinkedHashMap<String, Address> configuration = new LinkedHashMap<>();
      for (String nodeAddressPair : nodes.split(String.valueOf(nodesSeparator))) {
         final String[] pair = nodeAddressPair.split(String.valueOf(nodeAddressSeparator));
         final String nodeID = pair[0];
         final String address = pair[1];
         configuration.put(nodeID, Address.from(address));
      }
      return configuration;
   }

}