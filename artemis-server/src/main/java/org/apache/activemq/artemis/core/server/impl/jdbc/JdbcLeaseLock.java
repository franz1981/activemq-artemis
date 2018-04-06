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
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.jboss.logging.Logger;

/**
 * JDBC implementation of a {@link LeaseLock} with a {@code String} defined {@link #holderId()}.
 */
final class JdbcLeaseLock implements LeaseLock {

   private static final Logger LOGGER = Logger.getLogger(JdbcLeaseLock.class);
   private static final int MAX_HOLDER_ID_LENGTH = 128;
   private final Connection connection;
   private final String holderId;
   private final PreparedStatement tryAcquireLock;
   private final PreparedStatement tryReleaseLock;
   private final PreparedStatement renewLock;
   private final PreparedStatement isLocked;
   private final PreparedStatement currentDateTime;
   private final long expirationMillis;
   private boolean maybeAcquired;
   private final String lockName;

   /**
    * The lock will be responsible (ie {@link #close()}) of all the {@link PreparedStatement}s used by it, but not of the {@link Connection},
    * whose life cycle will be managed externally.
    */
   JdbcLeaseLock(String holderId,
                 Connection connection,
                 PreparedStatement tryAcquireLock,
                 PreparedStatement tryReleaseLock,
                 PreparedStatement renewLock,
                 PreparedStatement isLocked,
                 PreparedStatement currentDateTime,
                 long expirationMIllis,
                 String lockName) {
      if (holderId.length() > MAX_HOLDER_ID_LENGTH) {
         throw new IllegalArgumentException("holderId length must be <=" + MAX_HOLDER_ID_LENGTH);
      }
      this.holderId = holderId;
      this.tryAcquireLock = tryAcquireLock;
      this.tryReleaseLock = tryReleaseLock;
      this.renewLock = renewLock;
      this.isLocked = isLocked;
      this.currentDateTime = currentDateTime;
      this.expirationMillis = expirationMIllis;
      this.maybeAcquired = false;
      this.connection = connection;
      this.lockName = lockName;
   }

   public String holderId() {
      return holderId;
   }

   @Override
   public long expirationMillis() {
      return expirationMillis;
   }

   private String readableLockStatus() {
      try {
         String result;
         connection.setAutoCommit(false);
         try {
            final PreparedStatement preparedStatement = this.isLocked;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
               if (!resultSet.next()) {
                  result = null;
               } else {
                  final String currentHolderId = resultSet.getString(1);
                  //warn about any zombie lock
                  final Timestamp expirationTime = resultSet.getTimestamp(2);
                  final Timestamp currentTimestamp = resultSet.getTimestamp(3);
                  result = "holderId = " + currentHolderId + " expirationTime = " + expirationTime + " currentTimestamp = " + currentTimestamp;
               }
            }
         } catch (SQLException ie) {
            connection.rollback();
            connection.setAutoCommit(true);
            return ie.getMessage();
         }
         connection.commit();
         connection.setAutoCommit(true);
         return result;
      } catch (SQLException e) {
         return e.getMessage();
      }
   }

   private long dbCurrentTimeMillis() throws SQLException {
      final long start = System.nanoTime();
      try (ResultSet resultSet = currentDateTime.executeQuery()) {
         resultSet.next();
         final Timestamp currentTimestamp = resultSet.getTimestamp(1);
         final long elapsedTime = System.nanoTime() - start;
         if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("[ " + lockName + " ] holderId = " + holderId + " currentTimestamp = " + currentTimestamp +
                            "  and it tooks " + TimeUnit.NANOSECONDS.toMillis(elapsedTime) + " ms");
         }
         return currentTimestamp.getTime();
      }
   }

   @Override
   public boolean renew() {
      synchronized (connection) {
         try {
            final boolean renewed;
            connection.setAutoCommit(false);
            try {
               final PreparedStatement preparedStatement = this.renewLock;
               final long now = dbCurrentTimeMillis();
               final Timestamp expirationTime = new Timestamp(now + expirationMillis);
               if (LOGGER.isDebugEnabled()) {
                  LOGGER.debug("[ " + lockName + " ] Requested renew holderId = " + holderId + " expirationTime = " + expirationTime);
               }
               preparedStatement.setTimestamp(1, expirationTime);
               preparedStatement.setString(2, holderId);
               preparedStatement.setTimestamp(3, expirationTime);
               preparedStatement.setTimestamp(4, expirationTime);
               final int updatedRows = preparedStatement.executeUpdate();
               renewed = updatedRows == 1;
            } catch (SQLException ie) {
               connection.rollback();
               connection.setAutoCommit(true);
               throw new IllegalStateException(ie);
            }
            connection.commit();
            connection.setAutoCommit(true);
            if (LOGGER.isDebugEnabled()) {
               if (!renewed) {
                  LOGGER.debug("[ " + lockName + " ] Renew failed for holderId = " + holderId + " lock status = { " + readableLockStatus() + " }");
               } else {
                  LOGGER.debug("[ " + lockName + " ] Renew succeeded for holderId = " + holderId);
               }
            }
            return renewed;
         } catch (SQLException e) {
            throw new IllegalStateException(e);
         }
      }
   }

   @Override
   public boolean tryAcquire() {
      synchronized (connection) {
         try {
            final boolean acquired;
            connection.setAutoCommit(false);
            try {
               final PreparedStatement preparedStatement = tryAcquireLock;
               final long now = dbCurrentTimeMillis();
               preparedStatement.setString(1, holderId);
               final Timestamp expirationTime = new Timestamp(now + expirationMillis);
               preparedStatement.setTimestamp(2, expirationTime);
               preparedStatement.setTimestamp(3, expirationTime);
               if (LOGGER.isDebugEnabled()) {
                  LOGGER.debug("[ " + lockName + " ] Requested try acquire Lock holderId = " + holderId + " expirationTime " + expirationTime);
               }
               acquired = preparedStatement.executeUpdate() == 1;
            } catch (SQLException ie) {
               connection.rollback();
               connection.setAutoCommit(true);
               throw new IllegalStateException(ie);
            }
            connection.commit();
            connection.setAutoCommit(true);
            if (acquired) {
               this.maybeAcquired = true;
               if (LOGGER.isDebugEnabled()) {
                  LOGGER.debug("[ " + lockName + " ] Lock acquired holderId = " + holderId);
               }
            } else {
               if (LOGGER.isDebugEnabled()) {
                  LOGGER.debug("[ " + lockName + " ] Lock not acquired holderId = " + holderId + " lock status = { " + readableLockStatus() + " }");
               }
            }
            return acquired;
         } catch (SQLException e) {
            throw new IllegalStateException(e);
         }
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
      synchronized (connection) {
         try {
            boolean result;
            connection.setAutoCommit(false);
            try {
               final PreparedStatement preparedStatement = this.isLocked;
               try (ResultSet resultSet = preparedStatement.executeQuery()) {
                  if (!resultSet.next()) {
                     result = false;
                  } else {
                     final String currentHolderId = resultSet.getString(1);
                     result = holderIdFilter.test(currentHolderId);
                     //warn about any zombie lock
                     final Timestamp expirationTime = resultSet.getTimestamp(2);
                     final Timestamp currentTimestamp = resultSet.getTimestamp(3);
                     final long currentTimestampMillis = currentTimestamp.getTime();
                     if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("[ " + lockName + " ] Found lock with holderId =  " + currentHolderId + " expirationTime =  " + expirationTime + " currentTimestamp = " + currentTimestamp);
                     }
                     if (expirationTime != null) {
                        final long lockExpirationTime = expirationTime.getTime();
                        final long expiredBy = currentTimestampMillis - lockExpirationTime;
                        if (expiredBy > 0) {
                           result = false;
                           if (LOGGER.isDebugEnabled()) {
                              LOGGER.debug("[ " + lockName + " ] Found zombie lock with holderId =  " + currentHolderId + " expired by " + expiredBy + " ms");
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
         }
      }
   }

   @Override
   public void release() {
      synchronized (connection) {
         try {
            final boolean released;
            connection.setAutoCommit(false);
            try {
               final PreparedStatement preparedStatement = this.tryReleaseLock;
               preparedStatement.setString(1, holderId);
               released = preparedStatement.executeUpdate() == 1;
               //consider it as released to avoid on finalize to be reclaimed
               this.maybeAcquired = false;
            } catch (SQLException ie) {
               connection.rollback();
               connection.setAutoCommit(true);
               throw new IllegalStateException(ie);
            }
            connection.commit();
            connection.setAutoCommit(true);
            if (!released) {
               if (LOGGER.isDebugEnabled()) {
                  LOGGER.debug("[ " + lockName + " ] holderId = " + holderId + " has failed to release a lock. Lock status = { " + readableLockStatus() + " }");
               }
            } else {
               if (LOGGER.isDebugEnabled()) {
                  LOGGER.debug("[ " + lockName + " ] holderId = " + holderId + " has released a lock");
               }
            }
         } catch (SQLException e) {
            throw new IllegalStateException(e);
         }
      }
   }

   @Override
   public void close() throws SQLException {
      synchronized (connection) {
         //to avoid being called if not needed
         if (!this.tryReleaseLock.isClosed()) {
            try {
               if (this.maybeAcquired) {
                  release();
               }
            } finally {
               this.tryReleaseLock.close();
               this.tryAcquireLock.close();
               this.renewLock.close();
               this.isLocked.close();
               this.currentDateTime.close();
            }
         }
      }
   }

   @Override
   protected void finalize() throws Throwable {
      close();
   }

}
