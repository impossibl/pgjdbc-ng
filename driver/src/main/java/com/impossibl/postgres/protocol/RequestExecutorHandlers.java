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
/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2004-2011, PostgreSQL Global Development Group
 *
 *
 *-------------------------------------------------------------------------
 */
package com.impossibl.postgres.protocol;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.Registry;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.Await;
import com.impossibl.postgres.utils.BlockingReadTimeoutException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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

    private void rethrowError() throws IOException {
      if (error == null) return;
      if (error instanceof IOException) {
        throw (IOException) error;
      }
      if (error instanceof RuntimeException) {
        throw (RuntimeException) error;
      }
      throw new RuntimeException(error);
    }

    public void await(long timeout, TimeUnit unit) throws IOException {
      if (!Await.awaitUninterruptibly(timeout, unit, completed::await)) {
        throw new BlockingReadTimeoutException();
      }
      rethrowError();
    }

  }

  public static class SynchronizedResult extends Result implements RequestExecutor.SynchronizedHandler {

    @Override
    public void handleReady(TransactionStatus transactionStatus) {
      completed.countDown();
    }

  }

  public static class PrepareResult extends Result implements RequestExecutor.PrepareHandler {

    private TypeRef[] describedParameterTypes;
    private ResultField[] describedResultFields;

    public Type[] getDescribedParameterTypes(Context context) throws IOException {
      checkCompleted();

      List<Type> list = new ArrayList<>(describedParameterTypes.length);
      for (TypeRef ref : describedParameterTypes) {
        Type resolve = context.getRegistry().resolve(ref);
        list.add(resolve);
      }
      return list.toArray(new Type[0]);
    }

    public ResultField[] getDescribedResultFields(Context context) throws IOException {
      checkCompleted();

      Registry registry = context.getRegistry();

      List<ResultField> list = new ArrayList<>(describedResultFields.length);
      for (ResultField resultField : describedResultFields) {
        Type type = registry.resolve(resultField.getTypeRef());
        ResultField clone = new ResultField(
            resultField.getName(),
            resultField.getRelationId(),
            resultField.getRelationAttributeNumber(),
            type,
            resultField.getTypeLength(),
            resultField.getTypeModifier(),
            type != null ? type.getResultFormat() : resultField.getFormat()
        );
        list.add(clone);
      }

      return list.toArray(new ResultField[0]);
    }

    @Override
    public void handleComplete(TypeRef[] parameterTypes, ResultField[] resultFields, List<Notice> notices) {
      this.describedParameterTypes = parameterTypes;
      this.describedResultFields = resultFields;
      this.notices = notices;

      completed.countDown();
    }

  }

  public abstract static class AnyQueryResult extends SynchronizedResult {

    public abstract boolean isSuspended();
    public abstract ResultBatch getBatch();

  }

  public static class QueryResult extends AnyQueryResult implements RequestExecutor.ExtendedQueryHandler {

    private boolean synced;
    private boolean suspended;
    private ResultBatch resultBatch;

    public QueryResult() {
      this(true);
    }

    public QueryResult(boolean synced) {
      this.synced = synced;
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
    public void handleComplete(String command, Long rowsAffected, Long insertedOid, TypeRef[] parameterTypes, ResultField[] resultFields, RowDataSet rows, List<Notice> notices) {
      this.resultBatch = new ResultBatch(command, rowsAffected, insertedOid, resultFields, retain(rows));
      this.notices = notices;
      if (!synced) {
        completed.countDown();
      }
    }

    @Override
    public void handleSuspend(TypeRef[] parameterTypes, ResultField[] resultFields, RowDataSet rows, List<Notice> notices) {
      this.resultBatch = new ResultBatch(null, null, null, resultFields, retain(rows));
      this.notices = notices;

      suspended = true;

      completed.countDown();
    }

  }

  public static class ExecuteResult extends AnyQueryResult implements RequestExecutor.ExecuteHandler {

    private boolean synced;
    private boolean suspended;
    private ResultField[] describedResultFields;
    private ResultBatch resultBatch;

    public ExecuteResult(ResultField[] describedResultFields) {
      this(true, describedResultFields);
    }

    public ExecuteResult(boolean synced, ResultField[] describedResultFields) {
      this.synced = synced;
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
      if (!synced) {
        completed.countDown();
      }
    }

    @Override
    public void handleSuspend(RowDataSet rows, List<Notice> notices) {
      this.resultBatch = new ResultBatch(null, null, null, describedResultFields, retain(rows));

      suspended = true;

      completed.countDown();
    }

  }

  public static class CompositeQueryResults extends SynchronizedResult implements RequestExecutor.QueryHandler {

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

  }

}
