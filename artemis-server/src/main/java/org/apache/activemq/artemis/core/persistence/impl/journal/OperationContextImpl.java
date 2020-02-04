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
package org.apache.activemq.artemis.core.persistence.impl.journal;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.locks.StampedLock;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQExceptionType;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.journal.impl.SimpleWaitIOCallback;
import org.apache.activemq.artemis.core.persistence.OperationContext;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.utils.ExecutorFactory;

/**
 * Each instance of OperationContextImpl is associated with an executor (usually an ordered Executor).
 *
 * Tasks are hold until the operations are complete and executed in the natural order as soon as the operations are returned
 * from replication and storage.
 *
 * If there are no pending IO operations, the tasks are just executed at the callers thread without any context switch.
 *
 * So, if you are doing operations that are not dependent on IO (e.g NonPersistentMessages) you wouldn't have any context switch.
 */
public class OperationContextImpl extends StampedLock implements OperationContext {

   private static final ThreadLocal<OperationContext> threadLocalContext = new ThreadLocal<>();

   public static void clearContext() {
      OperationContextImpl.threadLocalContext.set(null);
   }

   public static final Optional<OperationContext> getExistingContext() {
      return Optional.ofNullable(OperationContextImpl.threadLocalContext.get());
   }

   public static final OperationContext getContext() {
      return getContext(null);
   }

   public static OperationContext getContext(final ExecutorFactory executorFactory) {
      OperationContext token = OperationContextImpl.threadLocalContext.get();
      if (token == null) {
         if (executorFactory == null) {
            return null;
         } else {
            token = new OperationContextImpl(executorFactory.getExecutor());
            OperationContextImpl.threadLocalContext.set(token);
         }
      }
      return token;
   }

   public static void setContext(final OperationContext context) {
      OperationContextImpl.threadLocalContext.set(context);
   }

   private List<TaskHolder> tasks;
   private List<TaskHolder> storeOnlyTasks;
   private final Deque<TaskHolder> holderPool;
   private final int holderPoolCapacity;

   private long minimalStore = Long.MAX_VALUE;
   private long minimalReplicated = Long.MAX_VALUE;
   private long minimalPage = Long.MAX_VALUE;

   private static final AtomicLongFieldUpdater<OperationContextImpl> STORE_LINE_UP = AtomicLongFieldUpdater.newUpdater(OperationContextImpl.class, "storeLineUp");
   private static final AtomicLongFieldUpdater<OperationContextImpl> REPLICATION_LINE_UP = AtomicLongFieldUpdater.newUpdater(OperationContextImpl.class, "replicationLineUp");
   private static final AtomicLongFieldUpdater<OperationContextImpl> PAGE_LINE_UP = AtomicLongFieldUpdater.newUpdater(OperationContextImpl.class, "pageLineUp");
   private volatile long storeLineUp = 0;
   private volatile long replicationLineUp = 0;
   private volatile long pageLineUp = 0;

   private long stored = 0;
   private long replicated = 0;
   private long paged = 0;

   private int errorCode = -1;

   private String errorMessage = null;

   private final Executor executor;

   private static final AtomicLongFieldUpdater<OperationContextImpl> EXECUTORS_COMPLETED = AtomicLongFieldUpdater.newUpdater(OperationContextImpl.class, "executorsCompleted");
   // TODO these 2 fields will likely be hit by false sharing and should be separated by at least 1 cache line!
   private long executorsStarted = 0;
   private volatile long executorsCompleted = 0;

   public OperationContextImpl(final Executor singleThreadedExecutor) {
      super();
      this.executor = singleThreadedExecutor;
      this.holderPoolCapacity = 32;
      this.holderPool = new ArrayDeque<>(holderPoolCapacity);
   }

   @Override
   public void pageSyncLineUp() {
      PAGE_LINE_UP.incrementAndGet(this);
   }

   @Override
   public void pageSyncDone() {
      final long stamp = spinWriteLock();
      try {
         paged++;
         checkTasks();
      } finally {
         unlockWrite(stamp);
      }
   }

   @Override
   public void storeLineUp() {
      STORE_LINE_UP.incrementAndGet(this);
   }

   @Override
   public void replicationLineUp() {
      REPLICATION_LINE_UP.incrementAndGet(this);
   }

   @Override
   public void replicationDone() {
      final long stamp = spinWriteLock();
      try {
         replicated++;
         checkTasks();
      } finally {
         unlockWrite(stamp);
      }
   }

   @Override
   public void executeOnCompletion(IOCallback runnable) {
      executeOnCompletion(runnable, false);
   }

   @Override
   public void executeOnCompletion(final IOCallback completion, final boolean storeOnly) {
      if (errorCode != -1) {
         completion.onError(errorCode, errorMessage);
         return;
      }

      boolean executeNow = false;

      final long stamp = spinWriteLock();
      try {
         final long UNDEFINED = Long.MIN_VALUE;
         long storeLined = UNDEFINED;
         long pageLined = UNDEFINED;
         long replicationLined = UNDEFINED;
         if (storeOnly) {
            if (storeOnlyTasks == null) {
               storeOnlyTasks = new LinkedList<>();
            }
         } else {
            if (tasks == null) {
               tasks = new LinkedList<>();
               minimalReplicated = (replicationLined = replicationLineUp);
               minimalStore = (storeLined = storeLineUp);
               minimalPage = (pageLined = pageLineUp);
            }
         }
         //On the next branches each of them is been used
         if (replicationLined == UNDEFINED) {
            replicationLined = replicationLineUp;
            storeLined = storeLineUp;
            pageLined = pageLineUp;
         }
         // On this case, we can just execute the context directly

         if (replicationLined == replicated && storeLined == stored && pageLined == paged) {
            // We want to avoid the executor if everything is complete...
            // However, we can't execute the context if there are executions pending
            // We need to use the executor on this case
            // Order matters here: loading first the consumer allows a conservative approach here in case
            // of a racing offering
            if (executorsCompleted == executorsStarted) {
               // No need to use an executor here or a context switch
               // there are no actions pending.. hence we can just execute the task directly on the same thread
               executeNow = true;
            } else {
               submitExecute(completion);
            }
         } else {
            if (storeOnly) {
               storeOnlyTasks.add(TaskHolder.with(holderPool, completion, storeLined, replicationLined, pageLined));
            } else {
               tasks.add(TaskHolder.with(holderPool, completion, storeLined, replicationLined, pageLined));
            }
         }
      } finally {
         unlockWrite(stamp);
      }

      if (executeNow) {
         // Executing outside of any locks
         completion.done();
      }

   }

   private long spinWriteLock() {
      return writeLock();
   }

   @Override
   public void done() {
      final long stamp = spinWriteLock();
      try {
         stored++;
         checkTasks();
      } finally {
         unlockWrite(stamp);
      }
   }

   private void checkTasks() {
      if (storeOnlyTasks != null && !storeOnlyTasks.isEmpty()) {
         Iterator<TaskHolder> iter = storeOnlyTasks.iterator();
         while (iter.hasNext()) {
            final TaskHolder holder = iter.next();
            if (stored >= holder.storeLined) {
               try {
                  // If set, we use an executor to avoid the server being single threaded
                  submitExecute(holder.task);
                  iter.remove();
               } finally {
                  holder.close(holderPool, holderPoolCapacity);
               }
            }
         }
      }

      if (stored >= minimalStore && replicated >= minimalReplicated && paged >= minimalPage) {
         if (tasks.isEmpty()) {
            return;
         }
         Iterator<TaskHolder> iter = tasks.iterator();
         while (iter.hasNext()) {
            final TaskHolder holder = iter.next();
            if (stored >= holder.storeLined && replicated >= holder.replicationLined && paged >= holder.pageLined) {
               try {
                  // If set, we use an executor to avoid the server being single threaded
                  submitExecute(holder.task);
                  iter.remove();
               } finally {
                  holder.close(holderPool, holderPoolCapacity);
               }
            } else {
               // End of list here. No other task will be completed after this
               break;
            }
         }
      }
   }

   /**
    * @param task
    */
   private void submitExecute(final IOCallback task) {
      try {
         executor.execute(() -> {
            try {
               // If any IO is done inside the callback, it needs to be done on a new context
               OperationContextImpl.clearContext();
               task.done();
            } finally {
               // this executor provide single-threaded semantic: there is no race here while doing this
               EXECUTORS_COMPLETED.lazySet(this, executorsCompleted + 1);
            }
         });
         // single writer: no need any fences here, because
         // the reader has acquired "this" lock
         executorsStarted++;
      } catch (Throwable e) {
         ActiveMQServerLogger.LOGGER.errorExecutingAIOCallback(e);
         task.onError(ActiveMQExceptionType.INTERNAL_ERROR.getCode(), "It wasn't possible to complete IO operation - " + e.getMessage());
      }
   }

   /*
    * (non-Javadoc)
    * @see org.apache.activemq.artemis.core.replication.ReplicationToken#complete()
    */
   public void complete() {
   }

   @Override
   public void onError(final int errorCode, final String errorMessage) {
      final long stamp = spinWriteLock();
      try {
         this.errorCode = errorCode;
         this.errorMessage = errorMessage;

         if (tasks != null && !tasks.isEmpty()) {
            Iterator<TaskHolder> iter = tasks.iterator();
            while (iter.hasNext()) {
               final TaskHolder holder = iter.next();
               try {
                  holder.task.onError(errorCode, errorMessage);
                  iter.remove();
               } finally {
                  holder.close(holderPool, holderPoolCapacity);
               }
            }
         }
      } finally {
         unlockWrite(stamp);
      }
   }

   static final class TaskHolder {

      long storeLined;
      long replicationLined;
      long pageLined;
      private IOCallback task;

      private TaskHolder init(IOCallback task, long storeLined, long replicationLined, long pageLined) {
         this.task = task;
         this.storeLined = storeLined;
         this.replicationLined = replicationLined;
         this.pageLined = pageLined;
         return this;
      }

      public void close(Deque<TaskHolder> pool, int poolCapacity) {
         this.storeLined = 0;
         this.replicationLined = 0;
         this.pageLined = 0;
         this.task = null;
         if (pool.size() < poolCapacity) {
            pool.addLast(this);
         }
      }

      @Override
      public String toString() {
         return "TaskHolder [" +
            "storeLined=" + storeLined + ", " +
            "replicationLined=" + replicationLined + ", " +
            "pageLined=" + pageLined + ", " +
            "task=" + task + "]";
      }

      public static TaskHolder with(Deque<TaskHolder> pool,
                                    IOCallback callback,
                                    long storeLined,
                                    long replicationLined,
                                    long pageLined) {
         TaskHolder holder = pool.pollLast();
         if (holder == null) {
            holder = new TaskHolder();
         }
         return holder.init(callback, storeLined, replicationLined, pageLined);
      }

   }

   @Override
   public void waitCompletion() throws Exception {
      waitCompletion(0);
   }

   @Override
   public boolean waitCompletion(final long timeout) throws InterruptedException, ActiveMQException {
      SimpleWaitIOCallback waitCallback = new SimpleWaitIOCallback();
      executeOnCompletion(waitCallback);
      complete();
      if (timeout == 0) {
         waitCallback.waitCompletion();
         return true;
      } else {
         return waitCallback.waitCompletion(timeout);
      }
   }

   @Override
   public String toString() {
      StringBuilder buffer = new StringBuilder();
      if (tasks != null) {
         for (TaskHolder hold : tasks) {
            buffer.append("Task = " + hold + "\n");
         }
      }

      return "OperationContextImpl [" + hashCode() + "] [minimalStore=" + minimalStore +
         ", storeLineUp=" +
         storeLineUp +
         ", stored=" +
         stored +
         ", minimalReplicated=" +
         minimalReplicated +
         ", replicationLineUp=" +
         replicationLineUp +
         ", replicated=" +
         replicated +
         ", paged=" +
         paged +
         ", minimalPage=" +
         minimalPage +
         ", pageLineUp=" +
         pageLineUp +
         ", errorCode=" +
         errorCode +
         ", errorMessage=" +
         errorMessage +
         ", executorsPending=" + executorsStarted +
         ", executor=" + this.executor +
         "]" + buffer.toString();
   }
}