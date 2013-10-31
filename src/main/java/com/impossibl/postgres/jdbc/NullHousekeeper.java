package com.impossibl.postgres.jdbc;

public class NullHousekeeper implements Housekeeper {

  public static final NullHousekeeper INSTANCE = new NullHousekeeper();

  @Override
  public <T> Object add(T referent, Runnable cleanup) {
    return null;
  }

  @Override
  public void remove(Object cleanupKey) {
  }

  @Override
  public void emptyQueue() {
  }

  @Override
  public boolean testCheckCleaned(int referentId) {
    return false;
  }

  @Override
  public void testClear() {
  }

}
