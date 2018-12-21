package com.impossibl.postgres.protocol;

import com.impossibl.postgres.system.Context;
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

import static java.util.Arrays.stream;
import static java.util.concurrent.TimeUnit.SECONDS;

import static io.netty.util.ReferenceCountUtil.retain;

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

    public TypeRef[] getDescribedParameterTypes() {
      return describedParameterTypes;
    }

    public Type[] getDescribedParameterTypes(Context context) {
      checkCompleted();

      return stream(describedParameterTypes).map(ref -> ref.getType(context)).toArray(Type[]::new);
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

  public static abstract class AnyQueryResult extends Result {

    public abstract boolean isSuspended();
    public abstract ResultBatch getBatch();

  }

  public static class QueryResult extends AnyQueryResult implements RequestExecutor.SimpleQueryHandler, RequestExecutor.QueryHandler {

    private boolean suspended;
    private ResultBatch resultBatch;

    @Override
    public boolean isSuspended() {
      return suspended;
    }

    @Override
    public ResultBatch getBatch() {
      checkCompleted();

      return resultBatch;
    }

    @Override
    public void handleComplete(String command, Long rowsAffected, Long insertedOid, TypeRef[] parameterTypes, ResultField[] resultFields, RowDataSet rows, List<Notice> notices) {
      this.resultBatch = new ResultBatch(command, rowsAffected, insertedOid, resultFields, retain(rows));
      this.notices = notices;

      completed.countDown();
    }

    @Override
    public void handleSuspend(TypeRef[] parameterTypes, ResultField[] resultFields, RowDataSet rows, List<Notice> notices) {
      this.resultBatch = new ResultBatch(null, null, null, resultFields, retain(rows));
      this.notices = notices;

      suspended = true;

      completed.countDown();
    }

    @Override
    public void handleReady() {
    }

  }

  public static class ExecuteResult extends AnyQueryResult implements RequestExecutor.ExecuteHandler {

    private boolean suspended;
    private ResultField[] describedResultFields;
    private ResultBatch resultBatch;

    public ExecuteResult(ResultField[] describedResultFields) {
      this.describedResultFields = describedResultFields;
    }

    @Override
    public boolean isSuspended() {
      return suspended;
    }

    @Override
    public ResultBatch getBatch() {
      checkCompleted();

      return resultBatch;
    }

    @Override
    public void handleComplete(String command, Long rowsAffected, Long insertedOid, RowDataSet rows, List<Notice> notices) {
      this.resultBatch = new ResultBatch(command, rowsAffected, insertedOid, describedResultFields, retain(rows));
      this.notices = notices;

      completed.countDown();
    }

    @Override
    public void handleSuspend(RowDataSet rows, List<Notice> notices) {
      this.resultBatch = new ResultBatch(null, null, null, describedResultFields, retain(rows));

      suspended = true;

      completed.countDown();
    }

  }

  public static class CompositeQueryResults extends Result implements RequestExecutor.SimpleQueryHandler {

    private List<ResultBatch> resultBatches;

    public CompositeQueryResults() {
      resultBatches = new ArrayList<>();
    }

    public List<ResultBatch> getBatches() {
      return resultBatches;
    }

    @Override
    public void handleComplete(String command, Long rowsAffected, Long insertedOid, TypeRef[] parameterTypes, ResultField[] resultFields, RowDataSet rows, List<Notice> notices) {
      resultBatches.add(new ResultBatch(command, rowsAffected, insertedOid, resultFields, retain(rows)));

      this.notices.addAll(notices);
    }

    @Override
    public void handleReady() {
      completed.countDown();
    }

  }

}
