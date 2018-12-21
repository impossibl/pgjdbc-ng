package com.impossibl.postgres.protocol.v30;

import com.impossibl.postgres.protocol.Notice;
import com.impossibl.postgres.protocol.ResultField;
import com.impossibl.postgres.protocol.TransactionStatus;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.BindComplete;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.CloseComplete;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.CommandComplete;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.CommandError;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.DataRow;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.EmptyQuery;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.FunctionResult;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.NoData;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.Notification;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.ParameterDescriptions;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.ParameterStatus;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.ParseComplete;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.PortalSuspended;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.ReadyForQuery;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.ReportNotice;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.RowDescription;
import com.impossibl.postgres.system.BasicContext;
import com.impossibl.postgres.types.TypeRef;

import java.lang.ref.WeakReference;

import io.netty.buffer.ByteBuf;

class ProtocolHandlers {

  static final ProtocolHandler SYNC = new Adapter() {

    public String toString() {
      return "SYNC";
    }

    @Override
    public Action readyForQuery(TransactionStatus txnStatus) {
      return Action.Complete;
    }

  };

  public static class Adapter implements
      ParameterDescriptions, RowDescription, NoData, DataRow, PortalSuspended,
      BindComplete, CloseComplete, CommandComplete, ParseComplete,
      FunctionResult, EmptyQuery, ReportNotice, CommandError, ReadyForQuery {

    @Override
    public Action readyForQuery(TransactionStatus txnStatus) {
      return Action.Resume;
    }

    @Override
    public Action parameterDescriptions(TypeRef[] types) {
      return Action.Resume;
    }

    @Override
    public Action rowDescription(ResultField[] fields) {
      return Action.Resume;
    }

    @Override
    public Action rowData(ByteBuf data) {
      return Action.Resume;
    }

    @Override
    public Action portalSuspended() {
      return Action.Resume;
    }

    @Override
    public Action noData() {
      return Action.Resume;
    }

    @Override
    public Action parseComplete() {
      return Action.Resume;
    }

    @Override
    public Action bindComplete() {
      return Action.Resume;
    }

    @Override
    public Action closeComplete() {
      return Action.Resume;
    }

    @Override
    public Action emptyQuery() {
      return Action.Resume;
    }

    @Override
    public Action functionResult(ByteBuf data) {
      return Action.Resume;
    }

    @Override
    public Action error(Notice notice) {
      return Action.Resume;
    }

    @Override
    public Action notice(Notice notice) {
      return Action.Resume;
    }

    @Override
    public Action commandComplete(String command, Long rowsAffected, Long insertedOid) {
      return Action.Resume;
    }

    @Override
    public void exception(Throwable cause) {
    }

  }

  public static class DefaultHander extends Adapter {

  }

}
