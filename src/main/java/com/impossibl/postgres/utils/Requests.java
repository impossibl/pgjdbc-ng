package com.impossibl.postgres.utils;

import com.impossibl.postgres.system.NoticeException;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class Requests {

  public static void awaitRequest(Future<?> future) throws IOException, NoticeException {
    while(true) {
      try {
        future.get();
        return;
      }
      catch (ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof IOException) {
          throw (IOException) cause;
        }
        else if (cause instanceof NoticeException) {
          throw (NoticeException) cause;
        }
        else if (cause instanceof RuntimeException) {
          throw (RuntimeException) cause;
        }
        else {
          throw new RuntimeException(cause);
        }
      }
      catch (InterruptedException ignored) {
        // try again...
      }
    }
  }

  public static boolean awaitRequest(Future<?> future, long timeout, TimeUnit unit) throws IOException, NoticeException {
    while(true) {
      try {
        future.get(timeout, unit);
        return true;
      }
      catch (TimeoutException e) {
        return false;
      }
      catch (ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof IOException) {
          throw (IOException) cause;
        }
        else if (cause instanceof NoticeException) {
          throw (NoticeException) cause;
        }
        else if (cause instanceof RuntimeException) {
          throw (RuntimeException) cause;
        }
        else {
          throw new RuntimeException(cause);
        }
      }
      catch (InterruptedException ignored) {
        // try again...
      }
    }
  }

}
