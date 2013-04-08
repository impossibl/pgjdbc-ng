package com.impossibl.postgres.protocol.v30;

import java.io.IOException;
import java.util.List;

import org.jboss.netty.buffer.ChannelBuffer;

import com.impossibl.postgres.protocol.Notice;
import com.impossibl.postgres.protocol.ResultField;
import com.impossibl.postgres.protocol.TransactionStatus;
import com.impossibl.postgres.protocol.TypeRef;



public interface ProtocolListener {

	boolean isComplete();

	void parseComplete() throws IOException;

	void parametersDescription(List<TypeRef> parameterTypes) throws IOException;

	void noData() throws IOException;

	void bindComplete() throws IOException;

	void rowDescription(List<ResultField> resultFields) throws IOException;

	void rowData(ChannelBuffer buffer) throws IOException;

	void functionResult(Object value) throws IOException;

	void emptyQuery() throws IOException;

	void portalSuspended() throws IOException;

	void commandComplete(String command, Long rowsAffected, Long oid) throws IOException;

	void closeComplete() throws IOException;

	void ready(TransactionStatus txStatus) throws IOException;

	void error(Notice error) throws IOException;

	void notice(Notice notice) throws IOException;

	void notification(int processId, String channelName, String payload) throws IOException;

	void backendKeyData(int processId, int secretKey) throws IOException;

	void authenticated(ProtocolImpl protocol) throws IOException;

	void authenticateKerberos(ProtocolImpl protocol) throws IOException;

	void authenticateClear(ProtocolImpl protocol) throws IOException;

	void authenticateCrypt(ProtocolImpl protocol) throws IOException;

	void authenticateMD5(ProtocolImpl protocol, byte[] salt) throws IOException;

	void authenticateSCM(ProtocolImpl protocol) throws IOException;

	void authenticateGSS(ProtocolImpl protocol) throws IOException;

	void authenticateGSSCont(ProtocolImpl protocol) throws IOException;

	void authenticateSSPI(ProtocolImpl protocol) throws IOException;

}
