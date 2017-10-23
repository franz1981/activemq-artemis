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

package org.apache.activemq.artemis.cli.commands.tools.journal;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.activemq.artemis.cli.commands.ActionContext;
import org.apache.activemq.artemis.cli.commands.tools.OptionalLocking;
import org.apache.activemq.artemis.cli.commands.util.JournalLatency;
import org.apache.activemq.artemis.core.config.impl.FileConfiguration;
import org.apache.activemq.artemis.core.server.JournalType;

@Command(name = "stat-journal", description = "Prints the write latency percentiles with the current data folder")
public class StatJournal extends OptionalLocking {

   @Option(name = "--block-size", description = "The block size for each write (default 4096)")
   public int size = 4 * 1024;

   @Option(name = "--writes", description = "The number of writes to be performed (default 250): please use a value > -XX:CompileThreshold with --no-sync")
   public int writes = 250;

   @Option(name = "--tries", description = "The number of tries for the test (default 5)")
   public int tries = 5;

   @Option(name = "--no-sync", description = "Disable sync")
   public boolean nosyncs = false;

   @Option(name = "--sync", description = "Enable syncs")
   public boolean syncs = false;

   @Option(name = "--journal-type", description = "Journal Type to be used (default from broker.xml)")
   public String journalType = null;

   @Option(name = "--sync-writes", description = "It will perform each write synchronously")
   public boolean syncWrites = false;

   @Option(name = "--file", description = "The file name to be used (default test.tmp)")
   public String fileName = "test.tmp";

   @Option(name = "--max-aio", description = "libaio.maxAIO to be used (default: configuration::getJournalMaxIO_AIO()")
   public int maxAIO = 0;

   @Override
   public Object execute(ActionContext context) throws Exception {
      super.execute(context);

      FileConfiguration fileConfiguration = getFileConfiguration();

      if (nosyncs) {
         fileConfiguration.setJournalDatasync(false);
      } else if (syncs) {
         fileConfiguration.setJournalDatasync(true);
      }

      if (journalType != null) {
         fileConfiguration.setJournalType(JournalType.getType(journalType));
      }

      System.out.println("Performing " + tries + " tests writing " + writes + " blocks of " + size + " on each test, sync=" + fileConfiguration.isJournalDatasync() + " with journalType = " + fileConfiguration.getJournalType());

      fileConfiguration.getJournalLocation().mkdirs();

      if (maxAIO <= 0) {
         maxAIO = fileConfiguration.getJournalMaxIO_AIO();
      }

      JournalLatency.measureWriteLatencyDistribution(fileConfiguration.getJournalLocation(), size, writes, tries, context.out, context.err, fileConfiguration.isJournalDatasync(), syncWrites, fileName, maxAIO, fileConfiguration.getJournalType());

      return null;
   }
}
