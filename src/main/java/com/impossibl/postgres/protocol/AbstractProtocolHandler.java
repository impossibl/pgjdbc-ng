package com.impossibl.postgres.protocol;

import java.io.IOException;
import java.util.List;

import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.DataInputStream;

public class AbstractProtocolHandler implements ProtocolHandler {

	@Override
	public boolean isComplete() {
		return true;
	}

	@Override
	public void ready(TransactionStatus txStatus) {
	}

	@Override
	public void parseComplete() {
	}

	@Override
	public void parametersDescription(List<Type> parameterTypes) {
	}

	@Override
	public void noData() {
	}

	@Override
	public void bindComplete() {
	}

	@Override
	public void rowDescription(List<ResultField> asList) {
	}

	@Override
	public void rowData(Protocol protocol, DataInputStream stream) throws IOException {
	}

	@Override
	public void functionResult(Object value) {
	}

	@Override
	public void emptyQuery() {
	}

	@Override
	public void portalSuspended() {
	}

	@Override
	public void commandComplete(String command, Integer rowsAffected, Integer oid) {
	}

	@Override
	public void closeComplete() {
	}

	@Override
	public void notification(int processId, String channelName, String payload) {
	}

	@Override
	public void error(Error error) {
	}

	@Override
	public void backendKeyData(int processId, int secretKey) {
	}

	@Override
	public void authenticated(Protocol protocol) throws IOException {
	}

	@Override
	public void authenticateKerberos(Protocol protocol) throws IOException {
	}

	@Override
	public void authenticateClear(Protocol protocol) throws IOException {
	}

	@Override
	public void authenticateCrypt(Protocol protocol) throws IOException {
	}

	@Override
	public void authenticateMD5(Protocol protocol, byte[] salt) throws IOException {
	}

	@Override
	public void authenticateSCM(Protocol protocol) throws IOException {
	}

	@Override
	public void authenticateGSS(Protocol protocol) throws IOException {
	}

	@Override
	public void authenticateGSSCont(Protocol protocol) throws IOException {
	}

	@Override
	public void authenticateSSPI(Protocol protocol) throws IOException {
	}

}
