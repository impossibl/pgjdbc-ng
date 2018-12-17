package com.impossibl.postgres.jdbc;


import java.util.concurrent.atomic.AtomicReference;


abstract class ExecutionTimerTask implements Runnable {

  enum State {
    NotStarted,
    Running,
    Cancelling,
    Completed
  }

  private final AtomicReference<State> state = new AtomicReference<>(State.NotStarted);
  private Thread thread;

  protected abstract void go();

  @Override
  public void run() {

    try {

      thread = Thread.currentThread();

      if (!state.compareAndSet(State.NotStarted, State.Running))
        return;

      go();

    }
    catch (Throwable e) {
      // Ignore...
    }
    finally {
      state.set(State.Completed);
      synchronized (state) {
        state.notify();
      }
    }

  }

  void cancel() {

    if (this.state.getAndSet(State.Cancelling) == State.Running) {

      thread.interrupt();

      synchronized (state) {

        while (state.get() == State.Cancelling) {
          try {
            state.wait();
          }
          catch (InterruptedException e) {
            // Ignore
          }
        }
      }

    }

  }

}
