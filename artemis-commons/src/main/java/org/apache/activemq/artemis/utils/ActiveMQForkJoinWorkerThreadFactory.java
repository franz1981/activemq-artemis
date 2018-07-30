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
package org.apache.activemq.artemis.utils;

import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public final class ActiveMQForkJoinWorkerThreadFactory implements ForkJoinPool.ForkJoinWorkerThreadFactory {

   private String groupName;

   private final AtomicInteger threadCount = new AtomicInteger(0);

   private final ReusableLatch active = new ReusableLatch(0);

   private final int threadPriority;

   private final boolean daemon;

   private final ClassLoader tccl;

   private final AccessControlContext acc;

   private final String prefix;

   /**
    * Construct a new instance.  The access control context of the calling thread will be the one used to create
    * new threads if a security manager is installed.
    *
    * @param groupName the name of the thread group to assign threads to by default
    * @param daemon    whether the created threads should be daemon threads
    * @param tccl      the context class loader of newly created threads
    */
   public ActiveMQForkJoinWorkerThreadFactory(final String groupName, final boolean daemon, final ClassLoader tccl) {
      this(groupName, "Thread-", daemon, tccl);
   }

   /**
    * Construct a new instance.  The access control context of the calling thread will be the one used to create
    * new threads if a security manager is installed.
    *
    * @param groupName the name of the thread group to assign threads to by default
    * @param daemon    whether the created threads should be daemon threads
    * @param tccl      the context class loader of newly created threads
    */
   public ActiveMQForkJoinWorkerThreadFactory(final String groupName,
                                              String prefix,
                                              final boolean daemon,
                                              final ClassLoader tccl) {
      this.groupName = groupName;

      this.prefix = prefix;

      this.threadPriority = Thread.NORM_PRIORITY;

      this.tccl = tccl;

      this.daemon = daemon;

      this.acc = AccessController.getContext();
   }

   @Override
   public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
      // create a thread in a privileged block if running with Security Manager
      if (acc != null) {
         return AccessController.doPrivileged(new ThreadCreateAction(pool), acc);
      } else {
         return createThread(pool);
      }
   }

   private final class ThreadCreateAction implements PrivilegedAction<ForkJoinWorkerThread> {

      private final ForkJoinPool target;

      private ThreadCreateAction(final ForkJoinPool target) {
         this.target = target;
      }

      @Override
      public ForkJoinWorkerThread run() {
         return createThread(target);
      }
   }

   /**
    * It will wait all threads to finish
    */
   public boolean join(int timeout, TimeUnit timeUnit) {
      try {
         return active.await(timeout, timeUnit);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         return false;
      }
   }

   private ForkJoinWorkerThread createThread(final ForkJoinPool pool) {
      active.countUp();
      final String threadName = prefix + threadCount.getAndIncrement() + " (" + groupName + ")";
      final ForkJoinWorkerThread t = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
      t.setName(threadName);
      t.setDaemon(daemon);
      t.setPriority(threadPriority);
      t.setContextClassLoader(tccl);
      return t;
   }

   public static ActiveMQForkJoinWorkerThreadFactory defaultThreadFactory() {
      String callerClassName = Thread.currentThread().getStackTrace()[2].getClassName();
      return new ActiveMQForkJoinWorkerThreadFactory(callerClassName, false, null);
   }

}
