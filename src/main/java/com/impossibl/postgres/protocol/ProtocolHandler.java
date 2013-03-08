package com.impossibl.postgres.protocol;

import java.io.IOException;
import java.util.List;

import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.DataInputStream;

public interface ProtocolHandler {
	
	boolean isComplete();

	void parseComplete() throws IOException;
	void parametersDescription(List<Type> parameterTypes) throws IOException;
	void noData() throws IOException;
	void bindComplete() throws IOException;
	void rowDescription(List<ResultField> resultFields) throws IOException;
	void rowData(Protocol protocol, DataInputStream stream) throws IOException;
	void functionResult(Object value) throws IOException;
	void emptyQuery() throws IOException;
	void portalSuspended() throws IOException;
	void commandComplete(String command, Integer rowsAffected, Integer oid) throws IOException;
	void closeComplete() throws IOException;

	void ready(TransactionStatus txStatus) throws IOException;
	void error(Error error) throws IOException;
	void notification(int processId, String channelName, String payload) throws IOException;

	void backendKeyData(int processId, int secretKey) throws IOException;
	
	void authenticated(Protocol protocol) throws IOException;
	void authenticateKerberos(Protocol protocol) throws IOException;
	void authenticateClear(Protocol protocol) throws IOException;
	void authenticateCrypt(Protocol protocol) throws IOException;
	void authenticateMD5(Protocol protocol, byte[] salt) throws IOException;
	void authenticateSCM(Protocol protocol) throws IOException;
	void authenticateGSS(Protocol protocol) throws IOException;
	void authenticateGSSCont(Protocol protocol) throws IOException;
	void authenticateSSPI(Protocol protocol) throws IOException;

}
