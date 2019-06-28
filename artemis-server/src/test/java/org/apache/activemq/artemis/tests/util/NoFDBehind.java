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

package org.apache.activemq.artemis.tests.util;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;

import com.sun.management.UnixOperatingSystemMXBean;
import org.apache.activemq.artemis.utils.Wait;
import org.jboss.logging.Logger;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

/**
 * This is useful to make sure you won't have leaking threads between tests
 */
public class NoFDBehind extends TestWatcher {

   private static Logger log = Logger.getLogger(NoFDBehind.class);

   /**
    * -1 on maxVariance means no check
    */
   public NoFDBehind(int maxVariance, long maxFiles) {

      this.maxVariance = maxVariance;
      this.maxFiles = maxFiles;
   }

   long fdBefore;
   int maxVariance;
   long maxFiles;

   static OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();

   public static long getOpenFD() {
      if (os instanceof UnixOperatingSystemMXBean) {
         return ((UnixOperatingSystemMXBean) os).getOpenFileDescriptorCount();
      } else {
         return 0;
      }
   }

   @Override
   protected void starting(Description description) {
      fdBefore = getOpenFD();
   }

   @Override
   protected void failed(Throwable e, Description description) {
   }

   @Override
   protected void succeeded(Description description) {
   }

   long getVariance() {

      long fdAfter = getOpenFD();

      long variance = fdAfter - fdBefore;

      return variance;
   }

   /**
    * Override to tear down your specific external resource.
    */
   @Override
   protected void finished(Description description) {

      long fdAfter = getOpenFD();
      long variance = getVariance();

      if (variance > 0) {
         log.warn("test " + description.toString() + " is leaving " + variance + " files open with a total number of files open = " + fdAfter);
         System.err.println("test " + description.toString() + " is leaving " + variance + " files open with a total number of files open = " + fdAfter);
      }

      if (maxVariance > 0) {
         Wait.assertTrue("The test " + description + " is leaving " + variance + " FDs open, which is more than " + maxVariance + " max open", () -> getVariance() < maxVariance, 5000, 0);
      }

      Wait.assertTrue("Too many open files", () -> getOpenFD() < maxFiles, 5000, 0);

   }

}
