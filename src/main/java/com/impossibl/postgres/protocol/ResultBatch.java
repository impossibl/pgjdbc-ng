package com.impossibl.postgres.protocol;

import com.impossibl.postgres.system.Context;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCountUtil;

public class ResultBatch extends AbstractReferenceCounted implements Iterable<ResultBatch.Row>, AutoCloseable {

  public static class Row {

    private ResultField[] fields;
    private RowData rowData;

    private Row(ResultField[] fields, RowData rowData) {
      this.fields = fields;
      this.rowData = rowData;
    }

    public RowData getData() {
      return rowData;
    }

    public Object getField(int fieldIndex, Context context, Class<?> targetClass, Object targetContext) throws IOException {
      return rowData.getField(fieldIndex, fields[fieldIndex], context, targetClass, targetContext);
    }

    public <T> T getField(int fieldIndex, Context context, Class<T> targetClass) throws IOException {
      return targetClass.cast(getField(fieldIndex, context, targetClass, null));
    }

  }

  private class RowIterator implements Iterator<Row> {

    private Iterator<RowData> resultsIterator;

    private RowIterator(Iterator<RowData> resultsIterator) {
      this.resultsIterator = resultsIterator;
    }

    @Override
    public boolean hasNext() {
      return resultsIterator.hasNext();
    }

    @Override
    public Row next() {
      return new Row(fields, resultsIterator.next());
    }

  }

  private String command;
  private Long rowsAffected;
  private Long insertedOid;
  private ResultField[] fields;
  private List<RowData> results;

  public ResultBatch(String command, Long rowsAffected, Long insertedOid, ResultField[] fields, List<RowData> results) {
    this.command = command;
    this.rowsAffected = rowsAffected;
    this.insertedOid = insertedOid;
    this.fields = fields;
    this.results = results;
  }

  public boolean isEmpty() {
    return results.isEmpty();
  }

  public String getCommand() {
    return command;
  }

  public Long getRowsAffected() {
    return rowsAffected;
  }

  public Long getInsertedOid() {
    return insertedOid;
  }

  public ResultField[] getFields() {
    return fields;
  }

  public List<RowData> getResults() {
    return results;
  }

  public Row getRow(int rowIndex) {
    return new Row(fields, results.get(rowIndex));
  }

  @Override
  protected void deallocate() {
    results.forEach(ReferenceCountUtil::release);
  }

  public ResultBatch touch(Object hint) {
    results.forEach(rowData -> ReferenceCountUtil.touch(rowData, hint));
    return this;
  }

  @Override
  public Iterator<Row> iterator() {
    return new RowIterator(results.iterator());
  }

  @Override
  public void close() {
    release();
  }

}
