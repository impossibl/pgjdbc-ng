package com.impossibl.postgres.protocol.v30;

import com.impossibl.postgres.protocol.Notice;
import com.impossibl.postgres.protocol.ResultField;
import com.impossibl.postgres.protocol.TransactionStatus;
import com.impossibl.postgres.types.TypeRef;

import java.io.IOException;
import java.util.List;

import io.netty.buffer.ByteBuf;

public interface ProtocolHandler {

  void exception(Throwable cause) throws IOException;


  enum Action {
    Resume,
    ResumePassing,
    Complete,
    CompletePassing,
    Sync,
  }

  interface NegotiateProtocolVersion extends ProtocolHandler {

    Action negotiate(int maxSupportedMinorVersion, List<String> unrecognizedParameters) throws IOException;

  }

  interface Authentication extends ProtocolHandler {

    Action authenticated() throws IOException;
    void authenticateKerberos(ProtocolChannel channel) throws IOException;
    void authenticateClear(ProtocolChannel channel) throws IOException;
    void authenticateMD5(byte[] salt, ProtocolChannel channel) throws IOException;
    void authenticateSCM(ProtocolChannel channel) throws IOException;
    void authenticateGSS(ByteBuf data, ProtocolChannel channel) throws IOException;
    void authenticateSSPI(ByteBuf data, ProtocolChannel channel) throws IOException;
    void authenticateContinue(ByteBuf data, ProtocolChannel channel) throws IOException;

  }

  interface BackendKeyData extends ProtocolHandler {

    Action backendKeyData(int processId, int secretKey) throws IOException;

  }

  interface ParameterStatus extends ProtocolHandler {

    Action parameterStatus(String name, String value) throws IOException;

  }

  interface ParameterDescriptions extends ProtocolHandler {

    Action parameterDescriptions(TypeRef[] types) throws IOException;

  }

  interface RowDescription extends ProtocolHandler {

    Action rowDescription(ResultField[] fields) throws IOException;

  }

  interface DataRow extends ProtocolHandler {

    Action rowData(ByteBuf data) throws IOException;

  }

  interface PortalSuspended extends ProtocolHandler {

    Action portalSuspended() throws IOException;

  }

  interface NoData extends ProtocolHandler {

    Action noData() throws IOException;

  }

  interface ParseComplete extends ProtocolHandler {

    Action parseComplete() throws IOException;

  }

  interface BindComplete extends ProtocolHandler {

    Action bindComplete() throws IOException;

  }

  interface CloseComplete extends ProtocolHandler {

    Action closeComplete() throws IOException;

  }

  interface EmptyQuery extends ProtocolHandler {

    Action emptyQuery() throws IOException;

  }

  interface FunctionResult extends ProtocolHandler {

    Action functionResult(ByteBuf data) throws IOException;

  }

  interface CommandError extends ProtocolHandler {

    Action error(Notice notice) throws IOException;

  }

  interface ReportNotice extends ProtocolHandler {

    Action notice(Notice notice) throws IOException;

  }

  interface CommandComplete extends ProtocolHandler {

    Action commandComplete(String command, Long rowsAffected, Long insertedOid) throws IOException;

  }

  interface ReadyForQuery extends ProtocolHandler {

    Action readyForQuery(TransactionStatus txnStatus) throws IOException;

  }

  interface Notification {

    void notification(int processId, String channelName, String payload) throws IOException;

  }

}
