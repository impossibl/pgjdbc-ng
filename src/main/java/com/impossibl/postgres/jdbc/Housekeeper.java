package com.impossibl.postgres.jdbc;

public interface Housekeeper {

  /**
   * Associate a cleanup runnable to be run when a referent is only phantom
   * reference-able.
   *
   * @param referent
   *          Reference to track
   * @param cleanup
   *          Runnable to run when referent is phantom-ed
   * @return Key object to use when calling {@link remove}
   */
  <T> Object add(T referent, Runnable cleanup);

  /**
   * Removes cleanup runnable for the given referent
   *
   * @param referent
   *          Reference to stop tracking
   */
  void remove(Object cleanupKey);

  /**
   * Ensures the cleanup queue is emptied immediately
   */
  void emptyQueue();

  /**
   * ** Only used for unit testing **
   *
   * Checks if a referent has been queued and then processed and removed from
   * the lists
   *
   * @param referent
   *          Referent to check
   * @return
   */
  boolean testCheckCleaned(int referentId);

  /**
   * ** Only used for unit testing **
   *
   * Clears list of references
   */
  void testClear();

}
