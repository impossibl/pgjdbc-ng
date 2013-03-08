package com.impossibl.postgres.system;

import static java.util.Arrays.asList;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

import com.impossibl.postgres.codecs.DateStyles;
import com.impossibl.postgres.codecs.DateTimeCodec;
import com.impossibl.postgres.codecs.StringCodec;
import com.impossibl.postgres.protocol.Error;
import com.impossibl.postgres.protocol.PrepareCommand;
import com.impossibl.postgres.protocol.Protocol;
import com.impossibl.postgres.protocol.ProtocolV30;
import com.impossibl.postgres.protocol.QueryCommand;
import com.impossibl.postgres.protocol.StartupCommand;
import com.impossibl.postgres.system.tables.PgAttribute;
import com.impossibl.postgres.system.tables.PgProc;
import com.impossibl.postgres.system.tables.PgType;
import com.impossibl.postgres.types.Registry;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.DataInputStream;
import com.impossibl.postgres.utils.DataOutputStream;
import com.impossibl.postgres.utils.Timer;

public class BasicContext implements Context {
	
	private static final Logger logger = Logger.getLogger(BasicContext.class.getName());
	
	
	public static class KeyData {
		int processId;
		int secretKey;
	}
	
	
	protected Registry registry;
	protected Map<String, Class<?>>  targetTypeMap;
	protected StringCodec stringCodec;
	protected DateTimeCodec dateTimeCodec;
	protected Properties settings;
	protected Version serverVersion;
	protected KeyData keyData;
	protected DataInputStream in;
	protected DataOutputStream out;
	protected Protocol protocol;
	protected Lock protocolLock;
	
	
	public BasicContext(Socket socket, Properties settings, Map<String, Class<?>> targetTypeMap) throws IOException {
		this.registry = new Registry(this);
		this.targetTypeMap = new HashMap<String, Class<?>>(targetTypeMap);
		this.settings = settings;
		this.stringCodec = new StringCodec((Charset) settings.get("client.encoding"));
		this.dateTimeCodec = new DateTimeCodec(DateFormat.getDateInstance(),TimeZone.getDefault());
		this.in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
		this.out = new DataOutputStream(socket.getOutputStream());
		this.protocol = new ProtocolV30(this);
		this.protocolLock = new ReentrantLock();
	}
	
	@Override
	public Registry getRegistry() {
		return registry;
	}

	public Protocol lockProtocol() {
		protocolLock.lock();
		return protocol;
	}
	
	public void unlockProtocol() {
		protocolLock.unlock();
	}
	
	@Override
	public DataInputStream getInputStream() {
		return in;
	}

	@Override
	public DataOutputStream getOutputStream() {
		return out;
	}

	@Override
	public Object getSetting(String name) {
		return settings.get(name);
	}

	public Class<?> lookupInstanceType(Type type) {
		
		Class<?> cls = targetTypeMap.get(type.getName());
		if(cls == null)
			cls = HashMap.class;
		
		return cls;
	}
	
	public StringCodec getStringCodec() {
		return stringCodec;
	}

	public DateTimeCodec getDateTimeCodec() {
		return dateTimeCodec;
	}

	public void refreshType(int typeId) {
	}
	
	public void init() throws IOException {
		
		if(start()) {
			
			loadTypes();
		}
	}

	private void loadTypes() throws IOException {
		
		Timer timer = new Timer();
		
		//Load types
		String typeSQL = PgType.INSTANCE.getSQL(serverVersion);
		List<PgType.Row> pgTypes = query(typeSQL, PgType.Row.class);
		
		//Load attributes
		String attrsSQL = PgAttribute.INSTANCE.getSQL(serverVersion);
		List<PgAttribute.Row> pgAttrs = query(attrsSQL, PgAttribute.Row.class);
		
		//Load procs
		String procsSQL = PgProc.INSTANCE.getSQL(serverVersion);
		List<PgProc.Row> pgProcs = query(procsSQL, PgProc.Row.class);
		
		logger.info("query time: " + timer.getLap() + "ms");

		//Update the registry with known types
		registry.update(pgTypes, pgAttrs, pgProcs);
		
		logger.info("load time: " + timer.getLap() + "ms");
	}
	
	private boolean start() throws IOException {
		
		Map<String, Object> params = new HashMap<String, Object>();

		params.put("application_name", "pgjdbc app");
		params.put("client_encoding", "UTF8");
		params.put("database", settings.get("database"));
		params.put("user", settings.get("username"));
		
		StartupCommand startup = new StartupCommand(params);
		
		startup.execute(this);
		
		return startup.getError() == null;
	}
	
	public <T> List<T> query(String queryTxt, Class<T> rowType, Object... params) throws IOException {

		PrepareCommand prepare = new PrepareCommand(null, queryTxt, Collections.<Type>emptyList());
		prepare.execute(this);
		
		QueryCommand query = new QueryCommand(null, null, prepare.getDescribedParameterTypes(), asList(params), rowType);
		
		query.execute(this);
		
		return query.getResults(rowType);
	}

	@Override
	public void setKeyData(int processId, int secretKey) {

		keyData = new KeyData();
		keyData.processId = processId;
		keyData.secretKey = secretKey;
	}

	@Override
	public void updateSystemParameter(String name, String value) {
		
		logger.info("system paramter: " + name + "=" + value);
		
		switch(name) {
		
		case "server_version":
			
			serverVersion = Version.parse(value);
			break;
			
		case "DateStyle":
			
			dateTimeCodec.setFormat(DateStyles.get(value));
			break;
			
		case "TimeZone":
			
			dateTimeCodec.setTimeZone(TimeZone.getTimeZone(value));
			break;
			
		case "integer_datetimes":

			settings.put("datetimes.binary.class", Integer.class);
			break;
			
		case "client_encoding":
			
			stringCodec.setCharset(Charset.forName(value));
			break;
		}
		
	}

	@Override
	public void reportNotification(int processId, String channelName, String payload) {
	}

	@Override
	public void reportNotice(byte type, String value) {
	}

	@Override
	public void reportError(Error error) {
	}

}
