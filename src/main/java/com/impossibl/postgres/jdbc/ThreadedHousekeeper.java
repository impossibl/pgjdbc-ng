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
import java.util.HashSet;
import java.util.Set;

import static java.util.Collections.synchronizedSet;



/**
 * Housekeeper that spins up a daemon thread to execute clean ups.
 * 
 * @author kdubb
 *
 */
public class ThreadedHousekeeper implements Housekeeper {

  private static class HousekeeperReference<T> extends PhantomReference<T> {

    int id;
    Runnable cleanup;

    public HousekeeperReference(Runnable cleanup, T referent, ReferenceQueue<? super T> q) {
      super(referent, q);

      if (cleanup == referent) {
        throw new IllegalArgumentException("target cannot be the referent");
      }

      this.id = System.identityHashCode(referent);
      this.cleanup = cleanup;
    }

    void cleanup() {
      cleanup.run();
    }

  }

  private ReferenceQueue<Object> cleanupQueue = new ReferenceQueue<>();
  private Set<HousekeeperReference<?>> cleanupReferences = synchronizedSet(new HashSet<HousekeeperReference<?>>());
  private Thread cleanupThread = new Thread() {

    @Override
    public void run() {

      while (true) {

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

        cleanupReferences.remove(ref);
      }

    }

  };

  public ThreadedHousekeeper() {
    cleanupThread.setName("PG-JDBC Housekeeper");
    cleanupThread.setDaemon(true);
    cleanupThread.start();
  }

  @Override
  public void emptyQueue() {

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

  private HousekeeperReference<?>[] copyCleanupReferences() {
    return cleanupReferences.toArray(new HousekeeperReference<?>[cleanupReferences.size()]);
  }

  @Override
  public <T> Object add(T referent, Runnable cleanup) {
    HousekeeperReference<T> ref = new HousekeeperReference<T>(cleanup, referent, cleanupQueue);
    cleanupReferences.add(ref);
    return cleanup;
  }

  @Override
  public void remove(Object cleanupKey) {

    HousekeeperReference<?>[] refs = copyCleanupReferences();
    for (HousekeeperReference<?> ref : refs) {

      if (ref.cleanup == cleanupKey) {
        cleanupReferences.remove(ref);
        return;
      }
    }

  }

  @Override
  public boolean testCheckCleaned(int referentId) {

    System.gc();

    // Ensure queue is emptied before checking
    emptyQueue();

    HousekeeperReference<?>[] refs = copyCleanupReferences();
    for (HousekeeperReference<?> ref : refs) {
      if (ref.id == referentId)
        return false;
    }

    return true;
  }

  @Override
  public void testClear() {
    cleanupReferences.clear();
  }

}
