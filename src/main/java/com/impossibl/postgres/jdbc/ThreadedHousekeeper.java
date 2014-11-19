/**
 * Copyright (c) 2013, impossibl.com
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  * Neither the name of impossibl.com nor the names of its contributors may
 *    be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package com.impossibl.postgres.jdbc;

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;



/**
 * Housekeeper that spins up a daemon thread to execute clean ups.
 *
 * @author kdubb
 *
 */
public class ThreadedHousekeeper implements Housekeeper {

  private static final Logger logger = Logger.getLogger(ThreadedHousekeeper.class.getName());

  private static long instanceRefs = 0;
  private static ThreadedHousekeeper instance;

  public class Ref implements Housekeeper.Ref {

    private boolean released;

    @Override
    public ThreadedHousekeeper get() {
      return ThreadedHousekeeper.this;
    }

    @Override
    public void release() {
      if (!released) {
        released = true;
        ThreadedHousekeeper.release();
      }
    }

    @Override
    public <T> Object add(T reference, CleanupRunnable cleanup) {
      return ThreadedHousekeeper.this.add(reference, cleanup);
    }

    @Override
    public void remove(Object cleanupKey) {
      ThreadedHousekeeper.this.remove(cleanupKey);
    }

  }

  public static synchronized Ref acquire() {

    if (instanceRefs == 0) {
      instance = new ThreadedHousekeeper();
    }
    ++instanceRefs;
    return instance.new Ref();
  }

  private static synchronized void release() {

    --instanceRefs;
    if (instanceRefs == 0) {
      instance.close();
      instance = null;
    }
  }

  private class HousekeeperReference<T> extends PhantomReference<T> {

    int id;
    CleanupRunnable cleanup;

    public HousekeeperReference(CleanupRunnable cleanup, T referent, ReferenceQueue<? super T> q) {
      super(referent, q);

      if (cleanup == referent) {
        throw new IllegalArgumentException("target cannot be the referent");
      }

      this.id = System.identityHashCode(referent);
      this.cleanup = cleanup;
    }

    void cleanup() {

      if (logLeaks) {
        String allocationTrace = printStackTrace(getSimplifiedAllocationStackTrace());
        logger.log(Level.WARNING, "cleaning up leaked " + cleanup.getKind() + "\n" + allocationTrace);
      }

      cleanup.run();
    }

    StackTraceElement[] getSimplifiedAllocationStackTrace() {

      StackTraceElement[] allocationTrace = cleanup.getAllocationStackTrace();

      // Find the first non driver related class
      for (int c = 0; c < allocationTrace.length; ++c) {

        StackTraceElement e = allocationTrace[c];

        String className = e.getClassName();
        String pkgName = className.substring(0, className.lastIndexOf('.'));

        String rootPkgName = "com.impossibl.postgres";
        String sqlPkgName = "java.sql";

        if ((!pkgName.startsWith(rootPkgName) && !pkgName.startsWith(sqlPkgName)) ||
            (pkgName.startsWith(rootPkgName) && className.endsWith("Test"))) {
          return Arrays.copyOfRange(allocationTrace, c, allocationTrace.length);

        }

      }

      return new StackTraceElement[0];
    }

    String printStackTrace(StackTraceElement[] trace) {

      StringBuilder sb = new StringBuilder();

      for (StackTraceElement traceElement : trace) {

        sb.append("  at ").append(traceElement).append('\n');

      }

      return sb.toString();
    }

  }

  private boolean logLeaks = true;
  private ReferenceQueue<Object> cleanupQueue = new ReferenceQueue<>();
  private Set<HousekeeperReference<?>> cleanupReferences = new HashSet<HousekeeperReference<?>>();
  private AtomicBoolean cleanupThreadEnabled = new AtomicBoolean(true);
  private Thread cleanupThread = new Thread() {

    @Override
    public void run() {

      while (cleanupThreadEnabled.get()) {

        HousekeeperReference<?> ref;
        try {
          ref = (HousekeeperReference<?>) cleanupQueue.remove();
        }
        catch (InterruptedException e1) {
          continue;
        }

        try {
          ref.cleanup();
        }
        catch (Throwable e) {
          // Ignore...
        }

        synchronized (ThreadedHousekeeper.this) {
          cleanupReferences.remove(ref);
        }
      }

    }

  };

  private ThreadedHousekeeper() {
    cleanupThread.setName("PG-JDBC Housekeeper");
    cleanupThread.setDaemon(true);
    cleanupThread.start();
  }

  @Override
  public void setLogLeakedReferences(boolean value) {
    logLeaks = value;
  }

  @Override
  public synchronized void emptyQueue() {

    HousekeeperReference<?> ref;

    while ((ref = (HousekeeperReference<?>) cleanupQueue.poll()) != null) {

      try {
        ref.cleanup();
      }
      catch (Throwable e) {
        // Ignore...
      }

      cleanupReferences.remove(ref);
    }

  }

  @Override
  public synchronized <T> Object add(T referent, CleanupRunnable cleanup) {
    HousekeeperReference<T> ref = new HousekeeperReference<T>(cleanup, referent, cleanupQueue);
    cleanupReferences.add(ref);
    return cleanup;
  }

  @Override
  public synchronized void remove(Object cleanupKey) {

    Iterator<HousekeeperReference<?>> refIter = cleanupReferences.iterator();
    while (refIter.hasNext()) {
      HousekeeperReference<?> ref = refIter.next();
      if (ref.cleanup == cleanupKey) {
        refIter.remove();
        return;
      }
    }

  }

  private synchronized void close() {

    cleanupThreadEnabled.set(false);
    cleanupThread.interrupt();

    try {
      cleanupThread.join();
    }
    catch (InterruptedException e) {
      // Not much to do
    }
  }

  /**
   * Test only
   */
  public synchronized boolean testCheckCleaned(int referentId) {

    System.gc();

    // Ensure queue is emptied before checking
    emptyQueue();

    for (HousekeeperReference<?> ref : cleanupReferences) {
      if (ref.id == referentId)
        return false;
    }

    return true;
  }

  /**
   * Test only
   */
  public synchronized void testClear() {
    cleanupReferences.clear();
  }

}
