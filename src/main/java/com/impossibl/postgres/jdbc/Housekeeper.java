package com.impossibl.postgres.jdbc;

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.HashSet;
import java.util.Set;
import static java.util.Collections.synchronizedSet;



public class Housekeeper {

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

  private static ReferenceQueue<Object> cleanupQueue = new ReferenceQueue<>();
  private static Set<HousekeeperReference<?>> cleanupReferences = synchronizedSet(new HashSet<HousekeeperReference<?>>());

  private static Thread cleanupThread = new Thread() {

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

  static {
    cleanupThread.setName("PG-JDBC Housekeeper");
    cleanupThread.setDaemon(true);
    cleanupThread.start();
  }

  private static void emptyQueue() {

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

  private static HousekeeperReference<?>[] copyCleanupReferences() {
    return cleanupReferences.toArray(new HousekeeperReference<?>[cleanupReferences.size()]);
  }

  /**
   * Associate a cleanup runnable to be run when a referent is only phantom
   * reference-able.
   * @param referent
   *          Reference to track
   * @param cleanup
   *          Runnable to run when referent is phantom-ed
   */
  public static <T> void add(T referent, Runnable cleanup) {
    HousekeeperReference<T> ref = new HousekeeperReference<T>(cleanup, referent, cleanupQueue);
    cleanupReferences.add(ref);
  }

  /**
   * Removes cleanup runnable for the given referent
   * @param referent
   *          Reference to stop tracking
   */
  public static <T> void remove(T referent) {

    int referentId = System.identityHashCode(referent);

    HousekeeperReference<?>[] refs = copyCleanupReferences();
    for (HousekeeperReference<?> ref : refs) {

      if (ref.id == referentId) {
        cleanupReferences.remove(ref);
      }
    }

  }

  /**
   * ** Only used for unit testing **
   * Checks if a referent has been queued and then processed and removed from
   * the lists
   * @param referent
   *          Referent to check
   * @return
   */
  public static boolean testCheckCleaned(int referentId) {

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

  /**
   * ** Only used for unit testing **
   * Clears list of references
   */
  public static void testClear() {
    cleanupReferences.clear();
  }

}
