package com.impossibl.postgres.protocol.v30;

import com.impossibl.postgres.protocol.FieldFormatRef;
import com.impossibl.postgres.protocol.Notice;
import com.impossibl.postgres.protocol.RequestExecutor.ExecuteHandler;
import com.impossibl.postgres.protocol.RowData;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.BindComplete;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.CommandComplete;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.CommandError;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.DataRow;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.EmptyQuery;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.NoData;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.PortalSuspended;
import com.impossibl.postgres.system.NoticeException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;


public class ExecuteStatementRequest implements ServerRequest {

  private String statementName;
  private String portalName;
  private FieldFormatRef[] parameterFormats;
  private ByteBuf[] parameterBuffers;
  private FieldFormatRef[] resultFieldFormats;
  private Integer maxRows;
  private ExecuteHandler handler;
  private List<RowData> rows;
  private List<Notice> notices;

  ExecuteStatementRequest(String statementName, String portalName,
                          FieldFormatRef[] parameterFormats, ByteBuf[] parameterBuffers,
                          FieldFormatRef[] resultFieldFormats, Integer maxRows,
                          ExecuteHandler handler) {
    this.statementName = statementName;
    this.portalName = portalName;
    this.parameterFormats = parameterFormats;
    this.parameterBuffers = parameterBuffers;
    this.resultFieldFormats = resultFieldFormats;
    this.maxRows = maxRows;
    this.handler = handler;
    this.rows = new ArrayList<>();
    this.notices = new ArrayList<>();
  }

  private class Handler implements BindComplete, NoData, DataRow, EmptyQuery, PortalSuspended, CommandComplete, CommandError {

    @Override
    public Action bindComplete() {
      return Action.Resume;
    }

    @Override
    public Action rowData(ByteBuf data) {
      rows.add(new BufferRowData(data.retain()));
      return Action.Resume;
    }

    @Override
    public Action noData() {
      return Action.Resume;
    }

    @Override
    public Action portalSuspended() throws IOException {

      try {
        handler.handleSuspend(rows, notices);
      }
      finally {
        rows.forEach(ReferenceCountUtil::release);
      }

      return Action.Sync;
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
        rows.forEach(ReferenceCountUtil::release);
      }

      return Action.Sync;
    }

    @Override
    public Action error(Notice error) throws IOException {

      try {
        handler.handleError(new NoticeException(error), notices);
      }
      finally {
        rows.forEach(ReferenceCountUtil::release);
      }

      return Action.Sync;
    }

    @Override
    public void exception(Throwable cause) throws IOException {

      try {
        handler.handleError(cause, notices);
      }
      finally {
        rows.forEach(ReferenceCountUtil::release);
      }

    }

  }

  @Override
  public ProtocolHandler createHandler() {
    return new Handler();
  }

  @Override
  public void execute(ProtocolChannel channel) throws IOException {

    int maxRows = this.maxRows != null ? this.maxRows : 0;

    channel.writeBind(portalName, statementName, parameterFormats, parameterBuffers, resultFieldFormats);
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
