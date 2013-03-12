package com.impossibl.postgres.system;

import static com.impossibl.postgres.system.Settings.FIELD_DATETIME_FORMAT_CLASS;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.logging.Logger;

import com.impossibl.postgres.protocol.BindExecCommand;
import com.impossibl.postgres.protocol.Notice;
import com.impossibl.postgres.protocol.PrepareCommand;
import com.impossibl.postgres.protocol.Protocol;
import com.impossibl.postgres.protocol.QueryCommand;
import com.impossibl.postgres.protocol.StartupCommand;
import com.impossibl.postgres.protocol.v30.ProtocolFactoryImpl;
import com.impossibl.postgres.system.tables.PgAttribute;
import com.impossibl.postgres.system.tables.PgProc;
import com.impossibl.postgres.system.tables.PgType;
import com.impossibl.postgres.types.Registry;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.Timer;

public class BasicContext implements Context {
	
	private static final Logger logger = Logger.getLogger(BasicContext.class.getName());
	
	
	public static class KeyData {
		int processId;
		int secretKey;
	}
	
	
	protected Registry registry;
	protected Map<String, Class<?>> targetTypeMap;
	protected Charset charset;
	protected TimeZone timeZone;
	protected Properties settings;
	protected Version serverVersion;
	protected KeyData keyData;
	protected Protocol protocol;
	
	public BasicContext(SocketAddress address, Properties settings, Map<String, Class<?>> targetTypeMap) throws IOException {
		this.registry = new Registry(this);
		this.targetTypeMap = new HashMap<String, Class<?>>(targetTypeMap);
		this.settings = settings;
		this.charset = UTF_8;
		this.timeZone = TimeZone.getTimeZone("UTC");
		this.protocol = new ProtocolFactoryImpl().connect(address, this);
	}
	
	protected void shutdown() {		
		
		protocol.shutdown();
		
		//Release resources
		protocol = null;
		registry = null;
		targetTypeMap = null;
		settings = null;
		serverVersion = null;
		keyData = null;
	}

	@Override
	public Registry getRegistry() {
		return registry;
	}

	public Protocol getProtocol() {
		return protocol;
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
	
	public Charset getCharset() {
		return charset;
	}

	public TimeZone getTimeZone() {
		return timeZone;
	}

	public void refreshType(int typeId) {
	}
	
	public void init() throws IOException, NoticeException {
		
		start();
			
		loadTypes();
	}

	private void loadTypes() throws IOException {
		
		Timer timer = new Timer();
		
		//Load types
		String typeSQL = PgType.INSTANCE.getSQL(serverVersion);
		List<PgType.Row> pgTypes = execQuery(typeSQL, PgType.Row.class);
		
		//Load attributes
		String attrsSQL = PgAttribute.INSTANCE.getSQL(serverVersion);
		List<PgAttribute.Row> pgAttrs = execQuery(attrsSQL, PgAttribute.Row.class);
		
		//Load procs
		String procsSQL = PgProc.INSTANCE.getSQL(serverVersion);
		List<PgProc.Row> pgProcs = execQuery(procsSQL, PgProc.Row.class);
		
		logger.info("query time: " + timer.getLap() + "ms");

		//Update the registry with known types
		registry.update(pgTypes, pgAttrs, pgProcs);
		
		logger.info("load time: " + timer.getLap() + "ms");
	}
	
	private void start() throws IOException, NoticeException {
		
		Map<String, Object> params = new HashMap<String, Object>();

		params.put("application_name", "pgjdbc app");
		params.put("client_encoding", "UTF8");
		params.put("database", settings.get("database"));
		params.put("user", settings.get("username"));
		
		StartupCommand startup = protocol.createStartup(params);

		protocol.execute(startup);
		
		Notice error = startup.getError();
		if (error != null) {
			throw new NoticeException("Startup Failed", error);
		}
	}
	
	public <T> List<T> execQuery(String queryTxt, Class<T> rowType, Object... params) throws IOException {

		PrepareCommand prepare = protocol.createPrepare(null, queryTxt, Collections.<Type>emptyList());
		
		protocol.execute(prepare);
		
		BindExecCommand query = protocol.createBindExec(null, null, prepare.getDescribedParameterTypes(), asList(params), prepare.getDescribedResultFields(), rowType);
		
		protocol.execute(query);
		
		return query.getResults(rowType);
	}

	public List<Object[]> execQuery(String queryTxt) throws IOException {

		QueryCommand query = protocol.createQuery(queryTxt);
		
		protocol.execute(query);
		
		@SuppressWarnings("unchecked")
		List<Object[]> res = (List<Object[]>) query.getResults();
		
		return res;
	}

	public Object execQueryForResult(String queryTxt) throws IOException {
		
		QueryCommand query = protocol.createQuery(queryTxt);
		
		protocol.execute(query);
		
		@SuppressWarnings("unchecked")
		List<Object[]> res = (List<Object[]>) query.getResults();
		if(res.isEmpty()) {
			return query.getResultRowsAffected();
		}
		
		Object[] firstRow = res.get(0);
		if(firstRow.length == 0)
			return null;
		
		return firstRow[0];
	}
	
	public String execQueryForString(String queryTxt) throws IOException {

		List<Object[]> res = execQuery(queryTxt);
		if(res.isEmpty()) {
			return "";
		}
		
		Object[] firstRow = res.get(0);
		if(firstRow.length == 0)
			return "";
		
		if(firstRow[0] == null)
			return "";
		
		return firstRow[0].toString();
	}

	public void setKeyData(int processId, int secretKey) {

		keyData = new KeyData();
		keyData.processId = processId;
		keyData.secretKey = secretKey;
	}

	public void updateSystemParameter(String name, String value) {
		
		logger.info("system paramter: " + name + "=" + value);
		
		switch(name) {
		
		case "server_version":
			
			serverVersion = Version.parse(value);
			break;
			
		case "DateStyle":

			break;
			
		case "TimeZone":
			
			timeZone = TimeZone.getTimeZone(value);
			break;
			
		case "integer_datetimes":

			settings.put(FIELD_DATETIME_FORMAT_CLASS, Integer.class);
			break;
			
		case "client_encoding":
			
			charset = Charset.forName(value);
			break;
		}
		
	}

	public void reportNotification(int processId, String channelName, String payload) {
	}

	public void reportNotice(byte type, String value) {
	}

	public void reportError(Notice error) {
	}

}
