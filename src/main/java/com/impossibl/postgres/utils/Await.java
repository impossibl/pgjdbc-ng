package com.impossibl.postgres.utils;

import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class Await {

  public interface InterruptibleTimeoutFunction {
    boolean await(long timeout, TimeUnit timeoutUnits) throws InterruptedException;
  }

  public interface InterruptibleFunction {
    void await() throws InterruptedException;
  }

  public static boolean awaitUninterruptibly(long timeout, TimeUnit timeoutUnits, InterruptibleTimeoutFunction waiter) {

    if (timeout < 1) {
      timeout = Long.MAX_VALUE;
    }

    while (timeout > 0) {

      long start = System.currentTimeMillis();

      try {

        return waiter.await(timeout, timeoutUnits);

      }
      catch (InterruptedException e) {
        // Ignore
      }

      timeout -= timeoutUnits.convert(System.currentTimeMillis() - start, MILLISECONDS);

    }

    return false;
  }

  public static void awaitUninterruptibly(InterruptibleFunction waiter) {

    while (true) {

      try {

        waiter.await();

        return;
      }
      catch (InterruptedException e) {
        // Ignore
      }

    }

  }

}
