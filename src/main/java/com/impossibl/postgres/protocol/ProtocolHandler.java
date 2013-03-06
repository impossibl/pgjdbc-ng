package com.impossibl.postgres.protocol;

import java.io.IOException;
import java.util.List;

import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.DataInputStream;

public interface ProtocolHandler {
	
	boolean isComplete();

	void ready(TransactionStatus txStatus) throws IOException;

	void parseComplete() throws IOException;

	void parametersDescription(List<Type> parameterTypes) throws IOException;

	void noData() throws IOException;

	void bindComplete() throws IOException;

	void rowDescription(List<ResultField> resultFields) throws IOException;

	void rowData(ProtocolV30 protocol, DataInputStream stream) throws IOException;

	void functionResult(Object value) throws IOException;

	void emptyQuery() throws IOException;

	void portalSuspended() throws IOException;

	void commandComplete(String commandTag) throws IOException;

	void closeComplete() throws IOException;

	void notification(int processId, String channelName, String payload) throws IOException;

	void error(Error error) throws IOException;

	void backendKeyData(int processId, int secretKey) throws IOException;

	void authenticated(ProtocolV30 protocol) throws IOException;

	void authenticateKerberos(ProtocolV30 protocol) throws IOException;

	void authenticateClear(ProtocolV30 protocol) throws IOException;

	void authenticateCrypt(ProtocolV30 protocol) throws IOException;

	void authenticateMD5(ProtocolV30 protocol, byte[] salt) throws IOException;

	void authenticateSCM(ProtocolV30 protocol) throws IOException;

	void authenticateGSS(ProtocolV30 protocol) throws IOException;

	void authenticateGSSCont(ProtocolV30 protocol) throws IOException;

	void authenticateSSPI(ProtocolV30 protocol) throws IOException;

}
