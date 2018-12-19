package com.impossibl.postgres.protocol.v30;

import com.impossibl.postgres.protocol.Notice;
import com.impossibl.postgres.protocol.RequestExecutor.ExecuteHandler;
import com.impossibl.postgres.protocol.RowDataSet;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.CommandComplete;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.CommandError;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.DataRow;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.EmptyQuery;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.PortalSuspended;
import com.impossibl.postgres.system.NoticeException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.ByteBuf;

import static io.netty.util.ReferenceCountUtil.release;


public class ResumePortalRequest implements ServerRequest {

  private String portalName;
  private int maxRows;
  private ExecuteHandler handler;
  private RowDataSet rows;
  private List<Notice> notices;

  ResumePortalRequest(String portalName, int maxRows, ExecuteHandler handler) {
    this.portalName = portalName;
    this.maxRows = maxRows;
    this.handler = handler;
    this.rows = new RowDataSet();
    this.notices = new ArrayList<>();
  }

  private class Handler implements DataRow, EmptyQuery, PortalSuspended, CommandComplete, CommandError {

    @Override
    public String toString() {
      return "Resume Portal";
    }

    @Override
    public Action rowData(ByteBuf data) {
      rows.add(new BufferRowData(data.retain()));
      return Action.Resume;
    }

    @Override
    public Action portalSuspended() throws IOException {

      try {
        handler.handleSuspend(rows, notices);
      }
      finally {
        release(rows);
      }

      return Action.Complete;
    }

    @Override
    public Action emptyQuery() throws IOException {
      return commandComplete(null, null, null);
    }

    @Override
    public Action commandComplete(String command, Long rowsAffected, Long insertedOid) throws IOException {

      try {
        handler.handleComplete(command, rowsAffected, insertedOid, rows, notices);
      }
      finally {
        release(rows);
      }

      return Action.Complete;
    }

    @Override
    public Action error(Notice error) throws IOException {

      try {
        handler.handleError(new NoticeException(error), notices);
      }
      finally {
        release(rows);
      }

      return Action.Sync;
    }

    @Override
    public void exception(Throwable cause) throws IOException {

      try {
        handler.handleError(cause, notices);
      }
      finally {
        release(rows);
      }

    }

  }

  @Override
  public Handler createHandler() {
    return new Handler();
  }

  @Override
  public void execute(ProtocolChannel channel) throws IOException {

    channel.writeExecute(portalName, maxRows);

    if (maxRows > 0) {
      channel.writeFlush();
    }
    else {
      channel.writeSync();
    }

    channel.flush();
  }

}
