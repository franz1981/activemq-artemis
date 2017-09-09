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

package org.apache.activemq.artemis.core.server.impl.jdbc;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.function.Supplier;

import org.apache.activemq.artemis.jdbc.store.sql.SQLProvider;
import org.apache.activemq.artemis.utils.UUID;

/**
 * JDBC implementation of a {@link SharedStateManager}.
 */
final class JdbcSharedStateManager implements SharedStateManager {

   private static final int MAX_CONNECTION_ATTEMPTS = 1;
   private static final int CONNECTION_VALIDATION_TIMEOUT = 2;
   private final Pool<Connection> connectionPool;
   private final LeaseLock liveLock;
   private final LeaseLock backupLock;
   private final SQLProvider sqlProvider;

   public static JdbcSharedStateManager usingDataSource(String holderId,
                                                        long locksExpirationMillis,
                                                        DataSource dataSource,
                                                        SQLProvider provider) {
      return new JdbcSharedStateManager(holderId, locksExpirationMillis, provider, () -> {
         try {
            return dataSource.getConnection();
         } catch (SQLException e) {
            throw new IllegalStateException(e);
         }
      });
   }

   static Driver uncheckedGetDriver(String jdbcDriverClass) {
      try {
         return (Driver) Class.forName(jdbcDriverClass).newInstance();
      } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
         throw new RuntimeException(e);
      }
   }

   public static JdbcSharedStateManager usingConnectionUrl(String holderId,
                                                           long locksExpirationMillis,
                                                           String jdbcConnectionUrl,
                                                           String jdbcDriverClass,
                                                           SQLProvider provider) {
      final Driver driver = uncheckedGetDriver(jdbcDriverClass);
      final Properties emptyProperties = new Properties();
      return new JdbcSharedStateManager(holderId, locksExpirationMillis, provider, () -> {
         try {
            final Connection connection = driver.connect(jdbcConnectionUrl, emptyProperties);
            if (connection == null) {
               throw new IllegalStateException("the driver: " + jdbcDriverClass + " isn't able to connect to the requested url: " + jdbcConnectionUrl);
            }
            return connection;
         } catch (SQLException e) {
            throw new IllegalStateException(e);
         }
      });
   }

   private JdbcSharedStateManager(String holderId,
                                  long locksExpirationMillis,
                                  SQLProvider sqlProvider,
                                  final Supplier<? extends Connection> connectionFactory) {
      this.connectionPool = Pool.unpooled(connectionFactory);
      this.sqlProvider = sqlProvider;
      this.liveLock = new JdbcLeaseLock(holderId, this.connectionPool, sqlProvider.tryLiveLockOnNodeManagerStoreTable(), sqlProvider.tryReleaseLiveLockOnNodeManagerStoreTable(), sqlProvider.renewLiveLockOnNodeManagerStoreTable(), sqlProvider.isLiveLockedOnNodeManagerStoreTable(), sqlProvider.currentTimestampOnNodeManagerStoreTable(), locksExpirationMillis, 0);
      this.backupLock = new JdbcLeaseLock(holderId, this.connectionPool, sqlProvider.tryBackupLockOnNodeManagerStoreTable(), sqlProvider.tryReleaseBackupLockOnNodeManagerStoreTable(), sqlProvider.renewBackupLockOnNodeManagerStoreTable(), sqlProvider.isBackupLockedOnNodeManagerStoreTable(), sqlProvider.currentTimestampOnNodeManagerStoreTable(), locksExpirationMillis, 0);
   }

   @Override
   public LeaseLock liveLock() {
      return this.liveLock;
   }

   @Override
   public LeaseLock backupLock() {
      return this.backupLock;
   }

   private UUID readNodeId(Connection connection) throws SQLException {
      try (PreparedStatement preparedStatement = connection.prepareStatement(this.sqlProvider.selectNodeIdOnNodeManagerStoreTable())) {
         try (ResultSet resultSet = preparedStatement.executeQuery()) {
            if (!resultSet.next()) {
               return null;
            } else {
               final String nodeId = resultSet.getString(1);
               if (nodeId != null) {
                  return new UUID(UUID.TYPE_TIME_BASED, UUID.stringToBytes(nodeId));
               } else {
                  return null;
               }
            }
         }
      }
   }

   @Override
   public UUID readNodeId() {
      final Connection connection = this.connectionPool.borrow();
      try {
         connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
         connection.setAutoCommit(true);
         final UUID nodeId = readNodeId(connection);
         return nodeId;
      } catch (SQLException e) {
         throw new IllegalStateException(e);
      } finally {
         connectionPool.release(connection);
      }
   }

   @Override
   public void writeNodeId(UUID nodeId) {
      final Connection connection = this.connectionPool.borrow();
      try {
         connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
         connection.setAutoCommit(true);
         try (PreparedStatement preparedStatement = connection.prepareStatement(this.sqlProvider.updateNodeIdOnNodeManagerStoreTable())) {
            preparedStatement.setString(1, nodeId.toString());
            if (preparedStatement.executeUpdate() != 1) {
               throw new IllegalStateException("can't write NODE_ID on the JDBC Node Manager Store!");
            }
         }
      } catch (SQLException e) {
         throw new IllegalStateException(e);
      } finally {
         this.connectionPool.release(connection);
      }
   }

   @Override
   public UUID setup(Supplier<? extends UUID> nodeIdFactory) {
      //uses a single transaction to make everything
      final Connection connection = connectionPool.borrow();
      try {
         final UUID nodeId;
         connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
         connection.setAutoCommit(false);
         try (ResultSet rs = connection.getMetaData().getTables(null, null, sqlProvider.getTableName(), null)) {
            //table doesn't exists
            if (!rs.next()) {
               try (Statement statement = connection.createStatement()) {
                  statement.executeUpdate(sqlProvider.createNodeManagerStoreTable());
                  statement.executeUpdate(sqlProvider.insertNodeIdOnNodeManagerStoreTable());
                  statement.executeUpdate(sqlProvider.insertStateOnNodeManagerStoreTable());
                  statement.executeUpdate(sqlProvider.insertLiveLockOnNodeManagerStoreTable());
                  statement.executeUpdate(sqlProvider.insertBackupLockOnNodeManagerStoreTable());
               }
               //write NodeId within the same transaction
               try (PreparedStatement preparedStatement = connection.prepareStatement(this.sqlProvider.updateNodeIdOnNodeManagerStoreTable())) {
                  nodeId = nodeIdFactory.get();
                  preparedStatement.setString(1, nodeId.toString());
                  preparedStatement.executeUpdate();
               }
            } else {
               //tables is present -> nodeId ,must be already populated
               nodeId = readNodeId(connection);
            }
         } catch (SQLException ie) {
            connection.rollback();
            connection.setAutoCommit(true);
            throw new IllegalStateException(ie);
         }
         connection.commit();
         connection.setAutoCommit(true);
         return nodeId;
      } catch (SQLException e) {
         throw new IllegalStateException(e);
      } finally {
         this.connectionPool.release(connection);
      }
   }

   private static State decodeState(String s) {
      if (s == null) {
         return State.NOT_STARTED;
      }
      switch (s) {
         case "L":
            return State.LIVE;
         case "F":
            return State.FAILING_BACK;
         case "P":
            return State.PAUSED;
         case "N":
            return State.NOT_STARTED;
         default:
            throw new IllegalStateException("unknown state [" + s + "]");
      }
   }

   private static String encodeState(State state) {
      switch (state) {
         case LIVE:
            return "L";
         case FAILING_BACK:
            return "F";
         case PAUSED:
            return "P";
         case NOT_STARTED:
            return "N";
         default:
            throw new IllegalStateException("unknown state [" + state + "]");
      }
   }

   @Override
   public State readState() {
      final Connection connection = this.connectionPool.borrow();
      try {
         connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
         connection.setAutoCommit(true);
         final State state;
         try (PreparedStatement preparedStatement = connection.prepareStatement(this.sqlProvider.selectStateOnNodeManagerStoreTable())) {
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
               if (!resultSet.next()) {
                  state = State.FIRST_TIME_START;
               } else {
                  state = decodeState(resultSet.getString(1));
               }
            }
         }
         return state;
      } catch (SQLException e) {
         throw new IllegalStateException(e);
      } finally {
         this.connectionPool.release(connection);
      }
   }

   @Override
   public void writeState(State state) {
      final String encodedState = encodeState(state);
      final Connection connection = this.connectionPool.borrow();
      try {
         connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
         connection.setAutoCommit(true);
         try (PreparedStatement preparedStatement = connection.prepareStatement(this.sqlProvider.updateStateOnNodeManagerStoreTable())) {
            preparedStatement.setString(1, encodedState);
            if (preparedStatement.executeUpdate() != 1) {
               throw new IllegalStateException("can't write STATE to the JDBC Node Manager Store!");
            }
         }
      } catch (SQLException e) {
         throw new IllegalStateException(e);
      } finally {
         this.connectionPool.release(connection);
      }
   }

}
