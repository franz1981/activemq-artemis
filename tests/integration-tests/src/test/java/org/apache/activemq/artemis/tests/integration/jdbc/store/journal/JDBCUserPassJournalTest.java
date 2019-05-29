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
package org.apache.activemq.artemis.tests.integration.jdbc.store.journal;

import org.apache.activemq.artemis.core.config.storage.DatabaseStorageConfiguration;
import org.apache.activemq.artemis.core.io.IOCriticalErrorListener;
import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.jdbc.store.drivers.JDBCUtils;
import org.apache.activemq.artemis.jdbc.store.journal.JDBCJournalImpl;
import org.apache.activemq.artemis.jdbc.store.sql.SQLProvider;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.ThreadLeakCheckRule;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.sql.DriverManager;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class JDBCUserPassJournalTest extends ActiveMQTestBase {

   @Rule
   public ThreadLeakCheckRule threadLeakCheckRule = new ThreadLeakCheckRule();

   private JDBCJournalImpl journal;

   private ScheduledExecutorService scheduledExecutorService;

   private ExecutorService executorService;

   private SQLProvider sqlProvider;

   private DatabaseStorageConfiguration dbConf;

   @After
   @Override
   public void tearDown() throws Exception {
      journal.destroy();
      try {
         DriverManager.getConnection("jdbc:derby:;shutdown=true;", "testuser", "testpassword");
      } catch (Exception ignored) {
      }
      System.clearProperty("derby.connection.requireAuthentication");
      System.clearProperty("derby.user.testuser");
      scheduledExecutorService.shutdown();
      scheduledExecutorService = null;
      executorService.shutdown();
      executorService = null;

   }

   @Before
   public void setup() throws Exception {
      System.setProperty("derby.connection.requireAuthentication", "true");
      System.setProperty("derby.user.testuser", "testpassword");
      dbConf = createDefaultDatabaseStorageConfiguration();
      dbConf.setJdbcUser("testuser");
      dbConf.setJdbcPassword("testpassword");
      sqlProvider = JDBCUtils.getSQLProvider(
         dbConf.getJdbcDriverClassName(),
         dbConf.getMessageTableName(),
         SQLProvider.DatabaseStoreType.MESSAGE_JOURNAL);
      scheduledExecutorService = new ScheduledThreadPoolExecutor(5);
      executorService = Executors.newSingleThreadExecutor();
      journal = new JDBCJournalImpl(dbConf.getJdbcConnectionUrl(), dbConf.getJdbcUser(), dbConf.getJdbcPassword(), dbConf.getJdbcDriverClassName(), sqlProvider, scheduledExecutorService, executorService, new IOCriticalErrorListener() {
         @Override
         public void onIOException(Throwable code, String message, SequentialFile file) {

         }
      }, 5);
      journal.start();
   }

   @Test
   public void testBasic() throws Exception {
      int noRecords = 10;
      for (int i = 0; i < noRecords; i++) {
         journal.appendAddRecord(i, (byte) 1, new byte[0], true);
      }

      assertEquals(noRecords, journal.getNumberOfRecords());
   }

}
