package com.impossibl.postgres.protocol;

import com.impossibl.postgres.system.NoticeException;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.types.TypeRef;
import com.impossibl.postgres.utils.Await;
import com.impossibl.postgres.utils.BlockingReadTimeoutException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;

import io.netty.util.ReferenceCountUtil;

public class RequestExecutorHandlers {

  public abstract static class Result implements RequestExecutor.ErrorHandler {

    protected Throwable error;
    protected List<Notice> notices = new ArrayList<>();
    protected CountDownLatch completed = new CountDownLatch(1);

    public boolean isValid() {
      checkCompleted();
      return error != null;
    }

    public Throwable getError() {
      checkCompleted();
      return error;
    }

    public List<Notice> getNotices() {
      checkCompleted();
      return notices;
    }

    @Override
    public void handleError(Throwable error, List<Notice> notices) {
      this.error = error;
      this.notices = notices;

      completed.countDown();
    }

    void checkCompleted() {
      try {
        if (completed.await(0, SECONDS))
          return;
      }
      catch (InterruptedException ignored) {
      }
      throw new IllegalStateException("Result has not completed.");
    }

    private void rethrowError() throws IOException, NoticeException {
      if (error == null) return;
      if (error instanceof IOException) {
        throw (IOException) error;
      }
      if (error instanceof NoticeException) {
        throw (NoticeException) error;
      }
      if (error instanceof RuntimeException) {
        throw (RuntimeException) error;
      }
      throw new RuntimeException(error);
    }

    public void await(long timeout, TimeUnit unit) throws IOException, NoticeException {
      if (!Await.awaitUninterruptibly(timeout, unit, completed::await)) {
        throw new BlockingReadTimeoutException();
      }
      rethrowError();
    }

  }

  public static class PrepareResult extends Result implements RequestExecutor.PrepareHandler {

    private TypeRef[] describedParameterTypes;
    private ResultField[] describedResultFields;

    public Type[] getDescribedParameterTypes() {
      checkCompleted();

      Type[] types = new Type[describedParameterTypes.length];
      for (int idx = 0; idx < types.length; ++idx)
        types[idx] = describedParameterTypes[idx].getType();
      return types;
    }

    public ResultField[] getDescribedResultFields() {
      checkCompleted();

      return describedResultFields;
    }

    @Override
    public void handleComplete(TypeRef[] parameterTypes, ResultField[] resultFields, List<Notice> notices) {
      this.describedParameterTypes = parameterTypes;
      this.describedResultFields = resultFields;
      this.notices = notices;

      completed.countDown();
    }

  }

  public static abstract class AnyQueryResults extends Result {

    public abstract ResultBatch getResultBatch();

  }

  public static class QueryResults extends AnyQueryResults implements RequestExecutor.QueryHandler {

    private ResultBatch resultBatch;

    @Override
    public ResultBatch getResultBatch() {
      checkCompleted();

      return resultBatch;
    }

    @Override
    public void handleComplete(String command, Long rowsAffected, Long insertedOid, TypeRef[] parameterTypes, ResultField[] resultFields, List<RowData> rows, List<Notice> notices) {
      this.resultBatch = new ResultBatch(command, rowsAffected, insertedOid, resultFields, rows);
      this.notices = notices;

      rows.forEach(ReferenceCountUtil::retain);

      completed.countDown();
    }

    @Override
    public void handleSuspend(TypeRef[] parameterTypes, ResultField[] resultFields, List<RowData> rows, List<Notice> notices) {
      this.resultBatch = new ResultBatch(null, null, null, resultFields, rows);

      completed.countDown();
    }

  }

  public static class ExecuteResults extends AnyQueryResults implements RequestExecutor.ExecuteHandler {

    private ResultField[] describedResultFields;
    private ResultBatch resultBatch;

    public ExecuteResults(ResultField[] describedResultFields) {
      this.describedResultFields = describedResultFields;
    }

    public ResultBatch getResultBatch() {
      checkCompleted();

      return resultBatch;
    }

    @Override
    public void handleComplete(String command, Long rowsAffected, Long insertedOid, List<RowData> rows, List<Notice> notices) {
      this.resultBatch = new ResultBatch(command, rowsAffected, insertedOid, describedResultFields, rows);
      this.notices = notices;

      rows.forEach(ReferenceCountUtil::retain);

      completed.countDown();
    }

    @Override
    public void handleSuspend(List<RowData> rows, List<Notice> notices) {
      this.resultBatch = new ResultBatch(null, null, null, describedResultFields, rows);

      completed.countDown();
    }

  }

}
