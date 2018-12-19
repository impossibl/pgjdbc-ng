package com.impossibl.postgres.protocol;

import java.util.ArrayList;
import java.util.List;

import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;

public class RowDataSet extends AbstractReferenceCounted {

  private List<RowData> rows;

  public RowDataSet() {
    this(0);
  }

  public RowDataSet(int capacity) {
    this.rows = new ArrayList<>(capacity);
  }

  public boolean isEmpty() {
    return rows.isEmpty();
  }

  public int size() {
    return rows.size();
  }

  public RowData borrow(int index) {
    return rows.get(index);
  }

  public void add(RowData row) {
    rows.add(row);
  }

  public RowData copy(int index) {
    return ReferenceCountUtil.retain(borrow(index));
  }

  public RowData take(int index) {
    return rows.remove(index);
  }

  public void remove(int index) {
    ReferenceCountUtil.release(rows.remove(index));
  }

  public List<RowData> borrowAll() {
    return rows;
  }

  @Override
  protected void deallocate() {
    rows.forEach(ReferenceCountUtil::release);
  }

  @Override
  public ReferenceCounted touch(Object hint) {
    rows.forEach(rowData -> ReferenceCountUtil.touch(rowData, hint));
    return this;
  }

  @Override
  public String toString() {
    if (rows.size() == 0) {
      return "Empty";
    }
    return rows.size() + " Rows";
  }

}
