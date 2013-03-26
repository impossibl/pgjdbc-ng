package com.impossibl.postgres.system;

import static com.impossibl.postgres.system.Settings.APPLICATION_NAME;
import static com.impossibl.postgres.system.Settings.CLIENT_ENCODING;
import static com.impossibl.postgres.system.Settings.CREDENTIALS_USERNAME;
import static com.impossibl.postgres.system.Settings.DATABASE;
import static com.impossibl.postgres.system.Settings.FIELD_DATETIME_FORMAT_CLASS;
import static com.impossibl.postgres.system.Settings.STANDARD_CONFORMING_STRINGS;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.net.SocketAddress;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.logging.Logger;

import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

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
import com.impossibl.postgres.types.Type.Category;
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
	protected DateTimeZone timeZone;
	protected DateTimeFormatter dateFormatter;
	protected DateTimeFormatter timeFormatter;
	protected DateTimeFormatter timestampFormatter;
	protected Properties settings;
	protected Version serverVersion;
	protected KeyData keyData;
	protected Protocol protocol;
	protected Set<WeakReference<NotificationListener>> notificationListeners;
	
	
	public BasicContext(SocketAddress address, Properties settings, Map<String, Class<?>> targetTypeMap) throws IOException {
		this.registry = new Registry(this);
		this.targetTypeMap = new HashMap<>(targetTypeMap);
		this.settings = settings;
		this.charset = UTF_8;
		this.timeZone = DateTimeZone.forID("UTC");
		this.dateFormatter = DateTimeFormat.fullDate();
		this.timeFormatter = DateTimeFormat.fullTime();
		this.timestampFormatter = DateTimeFormat.fullDateTime();
		this.protocol = new ProtocolFactoryImpl().connect(address, this);
		this.notificationListeners = new ConcurrentSkipListSet<>(); 
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

	public Version getServerVersion() {
		return serverVersion;
	}

	public void setServerVersion(Version serverVersion) {
		this.serverVersion = serverVersion;
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

	@Override
	public <T> T getSetting(String name, Class<T> type) {
		return type.cast(settings.get(name));
	}
	
	@Override
	public boolean isSettingEnabled(String name) {
		Object val = getSetting(name, Boolean.class);
		if(val instanceof String)
			return ((String)val).toLowerCase().equals("on");
		if(val instanceof Boolean)
			return (Boolean) val;
		return false;
	}

	public Class<?> lookupInstanceType(Type type) {
		
		Class<?> cls = targetTypeMap.get(type.getName());
		if(cls == null) {
			if(type.getCategory() == Category.Array)
				return Object[].class;
			else
				cls = HashMap.class;
		}
		
		return cls;
	}
	
	public Charset getCharset() {
		return charset;
	}

	public DateTimeZone getTimeZone() {
		return timeZone;
	}

	public DateTimeFormatter getDateFormatter() {
		return dateFormatter;
	}

	public DateTimeFormatter getTimeFormatter() {
		return timeFormatter;
	}

	public DateTimeFormatter getTimestampFormatter() {
		return timestampFormatter;
	}

	public void refreshType(int typeId) {
	}
	
	public void init() throws IOException, NoticeException {
		
		start();
			
		loadTypes();
	}

	private void loadTypes() throws IOException, NoticeException {
		
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

		params.put(APPLICATION_NAME, "pgjdbc app");
		params.put(CLIENT_ENCODING, "UTF8");
		params.put(DATABASE, settings.getProperty(DATABASE, ""));
		params.put(CREDENTIALS_USERNAME, settings.getProperty(CREDENTIALS_USERNAME, ""));
		
		StartupCommand startup = protocol.createStartup(params);

		protocol.execute(startup);
		
		Notice error = startup.getError();
		if (error != null) {
			throw new NoticeException("Startup Failed", error);
		}
	}
	
	protected <T> List<T> execQuery(String queryTxt, Class<T> rowType, Object... params) throws IOException, NoticeException {

		PrepareCommand prepare = protocol.createPrepare(0, queryTxt, Collections.<Type>emptyList());
		
		protocol.execute(prepare);
		
		if(prepare.getError() != null) {
			throw new NoticeException("Error preparing query", prepare.getError());
		}
		
		BindExecCommand query = protocol.createBindExec(0, 0, prepare.getDescribedParameterTypes(), asList(params), prepare.getDescribedResultFields(), rowType);
		
		protocol.execute(query);
		
		if(query.getError() != null) {
			throw new NoticeException("Error executing query", query.getError());
		}

		return query.getResults(rowType);
	}

	protected List<Object[]> execQuery(String queryTxt) throws IOException, NoticeException {

		QueryCommand query = protocol.createQuery(queryTxt);
		
		protocol.execute(query);
		
		if(query.getError() != null) {
			throw new NoticeException("Error querying", query.getError());
		}

		@SuppressWarnings("unchecked")
		List<Object[]> res = (List<Object[]>) query.getResults();
		
		return res;
	}

	protected Object execQueryForResult(String queryTxt) throws IOException, NoticeException {
		
		QueryCommand query = protocol.createQuery(queryTxt);
		
		protocol.execute(query);
		
		if(query.getError() != null) {
			throw new NoticeException("Error preparing query", query.getError());
		}

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
	
	protected String execQueryForString(String queryTxt) throws IOException, NoticeException {

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
			
			String[] parsedDateStyle = DateStyle.parse(value);
			
			if(parsedDateStyle == null) {
				logger.warning("Invalid DateStyle encountered");
			}
			else {
				
				dateFormatter = DateStyle.getDateFormatter(parsedDateStyle);
				if(dateFormatter == null) {
					logger.warning("Unknown Date format, reverting to default");
					dateFormatter = DateTimeFormat.fullDate();
				}
				dateFormatter = dateFormatter.withZone(timeZone);
				
				timeFormatter = DateStyle.getTimeFormatter(parsedDateStyle);
				if(timeFormatter == null) {
					logger.warning("Unknown Time format, reverting to default");
					timeFormatter = DateTimeFormat.fullTime();
				}
				timeFormatter = timeFormatter.withZone(timeZone);
				
				timestampFormatter = DateStyle.getTimestampFormatter(parsedDateStyle);
				if(timestampFormatter == null) {
					logger.warning("Unknown Timestamp format, reverting to default");
					timestampFormatter = DateTimeFormat.fullDateTime();
				}
				timestampFormatter = timestampFormatter.withZone(timeZone);
			}
			break;
			
		case "TimeZone":
			
			timeZone = DateTimeZone.forID(value);
			dateFormatter = dateFormatter.withZone(timeZone);
			timeFormatter = timeFormatter.withZone(timeZone);
			timestampFormatter = timestampFormatter.withZone(timeZone);
			break;
			
		case "integer_datetimes":

			settings.put(FIELD_DATETIME_FORMAT_CLASS, Integer.class);
			break;
			
		case "client_encoding":
			
			charset = Charset.forName(value);
			break;
			
		case STANDARD_CONFORMING_STRINGS:
			
			settings.put(STANDARD_CONFORMING_STRINGS, value.equals("on"));
			break;
			
		default:
			break;
		}
		
	}

	public void addNotificationListener(NotificationListener listener) {
		
		notificationListeners.add(new WeakReference<NotificationListener>(listener));
	}
	
	public void reportNotification(int processId, String channelName, String payload) {
		
		Iterator<WeakReference<NotificationListener>> iter = notificationListeners.iterator();
		while(iter.hasNext()) {
			
			NotificationListener listener = iter.next().get();
			if(listener == null) {
				
				iter.remove();
			}
			else {
				
				listener.notification(processId, channelName, payload);
			}
			
		}
		
	}

}
