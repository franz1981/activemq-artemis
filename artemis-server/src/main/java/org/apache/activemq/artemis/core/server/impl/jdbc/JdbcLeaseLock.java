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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.jboss.logging.Logger;

/**
 * JDBC implementation of a {@link LeaseLock} with a {@code String} defined {@link #holderId()}.
 */
final class JdbcLeaseLock implements LeaseLock {

   private static final Logger LOGGER = Logger.getLogger(JdbcLeaseLock.class);
   private static final int MAX_HOLDER_ID_LENGTH = 128;
   private final Pool<Connection> connectionPool;
   private final long maxAllowableMillisDiffFromDBTime;
   private long millisDiffFromCurrentTime;
   private final String holderId;
   private final String tryAcquireLockSql;
   private final String tryReleaseLockSql;
   private final String renewLockSql;
   private final String currentDateTimeSql;
   private final String isLockedSql;
   private final long expirationMillis;
   private boolean maybeAcquired;

   JdbcLeaseLock(String holderId,
                 Pool<Connection> connectionPool,
                 String tryAcquireLockSql,
                 String tryReleaseLockSql,
                 String renewLockSql,
                 String isLockedSql,
                 String currentDateTimeSql,
                 long expirationMIllis,
                 long maxAllowableMillisDiffFromDBTime) {
      if (holderId.length() > MAX_HOLDER_ID_LENGTH) {
         throw new IllegalArgumentException("holderId length must be <=" + MAX_HOLDER_ID_LENGTH);
      }
      this.holderId = holderId;
      this.maxAllowableMillisDiffFromDBTime = maxAllowableMillisDiffFromDBTime;
      this.millisDiffFromCurrentTime = Long.MAX_VALUE;
      this.tryAcquireLockSql = tryAcquireLockSql;
      this.tryReleaseLockSql = tryReleaseLockSql;
      this.renewLockSql = renewLockSql;
      this.isLockedSql = isLockedSql;
      this.currentDateTimeSql = currentDateTimeSql;
      this.expirationMillis = expirationMIllis;
      this.maybeAcquired = false;
      this.connectionPool = connectionPool;
   }

   JdbcLeaseLock(String holderId,
                 Supplier<? extends Connection> connectionFactory,
                 String tryAcquireLockSql,
                 String tryReleaseLockSql,
                 String renewLockSql,
                 String isLockedSql,
                 String currentDateTimeSql,
                 long expirationMIllis,
                 long maxAllowableMillisDiffFromDBTime) {
      this(holderId, Pool.unpooled(connectionFactory), tryAcquireLockSql, tryReleaseLockSql, renewLockSql, isLockedSql, currentDateTimeSql, expirationMIllis, maxAllowableMillisDiffFromDBTime);
   }

   public String holderId() {
      return holderId;
   }

   @Override
   public long expirationMillis() {
      return expirationMillis;
   }

   private long timeDifference(Connection connection) throws SQLException {
      if (Long.MAX_VALUE == millisDiffFromCurrentTime) {
         if (maxAllowableMillisDiffFromDBTime > 0) {
            millisDiffFromCurrentTime = determineTimeDifference(connection);
         } else {
            millisDiffFromCurrentTime = 0L;
         }
      }
      return millisDiffFromCurrentTime;
   }

   private long determineTimeDifference(final Connection connection) throws SQLException {
      try (PreparedStatement statement = connection.prepareStatement(currentDateTimeSql)) {
         try (ResultSet resultSet = statement.executeQuery()) {
            long result = 0L;
            if (resultSet.next()) {
               final Timestamp timestamp = resultSet.getTimestamp(1);
               final long diff = System.currentTimeMillis() - timestamp.getTime();
               if (Math.abs(diff) > maxAllowableMillisDiffFromDBTime) {
                  // off by more than maxAllowableMillisDiffFromDBTime so lets adjust
                  result = (-diff);
               }
               LOGGER.info(holderId() + " diff adjust from db: " + result + ", db time: " + timestamp);
            }
            return result;
         }
      }
   }

   @Override
   public boolean renew() {
      final Connection connection = this.connectionPool.borrow();
      try {
         final boolean result;
         connection.setAutoCommit(false);
         try {
            final long timeDifference = timeDifference(connection);
            try (PreparedStatement preparedStatement = connection.prepareStatement(renewLockSql)) {
               final long now = System.currentTimeMillis() + timeDifference;
               final Timestamp timestamp = new Timestamp(now + expirationMillis);
               preparedStatement.setTimestamp(1, timestamp);
               preparedStatement.setString(2, holderId);
               result = preparedStatement.executeUpdate() == 1;
            }
         } catch (SQLException ie) {
            connection.rollback();
            connection.setAutoCommit(true);
            throw new IllegalStateException(ie);
         }
         connection.commit();
         connection.setAutoCommit(true);
         return result;
      } catch (SQLException e) {
         throw new IllegalStateException(e);
      } finally {
         this.connectionPool.release(connection);
      }
   }

   @Override
   public boolean tryAcquire() {
      final Connection connection = this.connectionPool.borrow();
      try {
         final boolean acquired;
         connection.setAutoCommit(false);
         try {
            final long timeDifference = timeDifference(connection);
            try (PreparedStatement preparedStatement = connection.prepareStatement(tryAcquireLockSql)) {
               final long now = System.currentTimeMillis() + timeDifference;
               preparedStatement.setString(1, holderId);
               final Timestamp timestamp = new Timestamp(now + expirationMillis);
               preparedStatement.setTimestamp(2, timestamp);
               acquired = preparedStatement.executeUpdate() == 1;
            }
         } catch (SQLException ie) {
            connection.rollback();
            connection.setAutoCommit(true);
            throw new IllegalStateException(ie);
         }
         connection.commit();
         connection.setAutoCommit(true);
         if (acquired) {
            this.maybeAcquired = true;
         }
         return acquired;
      } catch (SQLException e) {
         throw new IllegalStateException(e);
      } finally {
         this.connectionPool.release(connection);
      }
   }

   @Override
   public boolean isHeld() {
      return checkValidHolderId(Objects::nonNull);
   }

   @Override
   public boolean isHeldByCaller() {
      return checkValidHolderId(this.holderId::equals);
   }

   private boolean checkValidHolderId(Predicate<? super String> holderIdFilter) {
      final Connection connection = this.connectionPool.borrow();
      try {
         boolean result;
         connection.setAutoCommit(false);
         try {
            final long timeDifference = timeDifference(connection);
            try (PreparedStatement preparedStatement = connection.prepareStatement(isLockedSql)) {
               try (ResultSet resultSet = preparedStatement.executeQuery()) {
                  if (!resultSet.next()) {
                     result = false;
                  } else {
                     final String currentHolderId = resultSet.getString(1);
                     result = holderIdFilter.test(currentHolderId);
                     //warn about any zombie lock
                     final Timestamp timestamp = resultSet.getTimestamp(2);
                     if (timestamp != null) {
                        final long lockExpirationTime = timestamp.getTime();
                        final long now = System.currentTimeMillis() + timeDifference;
                        final long expiredBy = now - lockExpirationTime;
                        if (expiredBy > 0) {
                           result = false;
                           LOGGER.warn("found zombie lock with holderId: " + currentHolderId + " expired by: " + expiredBy + " ms");
                        }
                     }
                  }
               }
            }
         } catch (SQLException ie) {
            connection.rollback();
            connection.setAutoCommit(true);
            throw new IllegalStateException(ie);
         }
         connection.commit();
         connection.setAutoCommit(true);
         return result;
      } catch (SQLException e) {
         throw new IllegalStateException(e);
      } finally {
         this.connectionPool.release(connection);
      }
   }

   @Override
   public void release() {
      final Connection connection = this.connectionPool.borrow();
      try {
         connection.setAutoCommit(false);
         try {
            try (PreparedStatement preparedStatement = connection.prepareStatement(tryReleaseLockSql)) {
               preparedStatement.setString(1, holderId);
               if (preparedStatement.executeUpdate() != 1) {
                  LOGGER.warn(holderId + " has failed to release a lock");
               } else {
                  this.maybeAcquired = false;
                  LOGGER.info(holderId + " has released a lock");
               }
            }
         } catch (SQLException ie) {
            connection.rollback();
            connection.setAutoCommit(true);
            throw new IllegalStateException(ie);
         }
         connection.commit();
         connection.setAutoCommit(true);
      } catch (SQLException e) {
         throw new IllegalStateException(e);
      } finally {
         this.connectionPool.release(connection);
      }
   }

   @Override
   protected void finalize() throws Throwable {
      //to avoid being called each time is collected
      if (this.maybeAcquired) {
         release();
      }
   }

}
