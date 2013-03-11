package com.impossibl.postgres.protocol.v30;

import java.io.IOException;
import java.util.List;

import org.jboss.netty.buffer.ChannelBuffer;

import com.impossibl.postgres.protocol.Error;
import com.impossibl.postgres.protocol.ResultField;
import com.impossibl.postgres.protocol.TransactionStatus;
import com.impossibl.postgres.types.Type;



public class BaseProtocolListener implements ProtocolListener {

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
	public void rowData(ChannelBuffer buffer) throws IOException {
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
	public void commandComplete(String command, Long rowsAffected, Long oid) {
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
	public void authenticated(ProtocolImpl protocol) throws IOException {
	}

	@Override
	public void authenticateKerberos(ProtocolImpl protocol) throws IOException {
	}

	@Override
	public void authenticateClear(ProtocolImpl protocol) throws IOException {
	}

	@Override
	public void authenticateCrypt(ProtocolImpl protocol) throws IOException {
	}

	@Override
	public void authenticateMD5(ProtocolImpl protocol, byte[] salt) throws IOException {
	}

	@Override
	public void authenticateSCM(ProtocolImpl protocol) throws IOException {
	}

	@Override
	public void authenticateGSS(ProtocolImpl protocol) throws IOException {
	}

	@Override
	public void authenticateGSSCont(ProtocolImpl protocol) throws IOException {
	}

	@Override
	public void authenticateSSPI(ProtocolImpl protocol) throws IOException {
	}

}
