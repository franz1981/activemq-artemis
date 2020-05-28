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

package org.apache.activemq.artemis.jdbc.store.file;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.activemq.artemis.jdbc.store.drivers.JDBCConnectionProvider;
import org.apache.activemq.artemis.jdbc.store.sql.SQLProvider;

@SuppressWarnings("SynchronizeOnNonFinalField")
public final class Db2SequentialFileDriver extends JDBCSequentialFileFactoryDriver {

   public Db2SequentialFileDriver() {
      super();
   }

   public Db2SequentialFileDriver(JDBCConnectionProvider connectionProvider, SQLProvider provider) {
      super(connectionProvider, provider);
   }

   @Override
   protected void prepareStatements() {
      this.deleteFile = sqlProvider.getDeleteFileSQL();
      this.createFile = sqlProvider.getInsertFileSQL();
      this.createFileColumnNames = new String[]{"ID"};
      this.selectFileByFileName = sqlProvider.getSelectFileByFileName();
      this.copyFileRecord = sqlProvider.getCopyFileRecordByIdSQL();
      this.renameFile = sqlProvider.getUpdateFileNameByIdSQL();
      this.readLargeObject = sqlProvider.getReadLargeObjectSQL();
      this.appendToLargeObject = sqlProvider.getAppendToLargeObjectSQL();
      this.selectFileNamesByExtension = sqlProvider.getSelectFileNamesByExtensionSQL();
   }

   @Override
   public int writeToFile(JDBCSequentialFile file, byte[] data) throws SQLException {
      if (data == null || data.length == 0) {
         return 0;
      }
      try (Connection connection = connectionProvider.getConnection()) {
         try (PreparedStatement appendToLargeObject = connection.prepareStatement(this.appendToLargeObject)) {
            connection.setAutoCommit(false);
            int bytesWritten;
            appendToLargeObject.setBytes(1, data);
            appendToLargeObject.setLong(2, file.getId());
            final int updatesFiles = appendToLargeObject.executeUpdate();
            assert updatesFiles <= 1;
            connection.commit();
            if (updatesFiles == 0) {
               bytesWritten = 0;
            } else {
               bytesWritten = data.length;
            }
            return bytesWritten;
         } catch (SQLException e) {
            connection.rollback();
            throw e;
         }
      }
   }
}
