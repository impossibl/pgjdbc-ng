package com.impossibl.postgres;

import static com.impossibl.postgres.types.Registry.loadType;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.logging.Logger;

import com.impossibl.postgres.codecs.DateStyles;
import com.impossibl.postgres.codecs.DateTimeCodec;
import com.impossibl.postgres.codecs.StringCodec;
import com.impossibl.postgres.protocol.Error;
import com.impossibl.postgres.protocol.Field;
import com.impossibl.postgres.protocol.QueryProtocol;
import com.impossibl.postgres.protocol.StartupProtocol;
import com.impossibl.postgres.protocol.TransactionStatus;
import com.impossibl.postgres.system.Version;
import com.impossibl.postgres.system.tables.PgAttribute;
import com.impossibl.postgres.system.tables.PgProc;
import com.impossibl.postgres.system.tables.PgType;
import com.impossibl.postgres.types.CompositeType.Attribute;
import com.impossibl.postgres.types.Registry;
import com.impossibl.postgres.types.TupleType;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.DataInputStream;
import com.impossibl.postgres.utils.DataOutputStream;

public class BasicContext implements Context {
	
	private static final Logger logger = Logger.getLogger(BasicContext.class.getName());
	
	
	public static class KeyData {
		int processId;
		int secretKey;
	}
	
	
	private Map<String, Class<?>>  targetTypeMap;
	private StringCodec stringCodec;
	private DateTimeCodec dateTimeCodec;
	private Map<String, Object> settings;
	private Version serverVersion;
	private KeyData keyData;
	private DataInputStream in;
	private DataOutputStream out;
	
	
	public BasicContext(Socket socket, Map<String, Object> settings, Map<String, Class<?>> targetTypeMap) throws IOException {
		this.targetTypeMap = new HashMap<String, Class<?>>(targetTypeMap);
		this.settings = new HashMap<String, Object>(settings);
		this.stringCodec = new StringCodec((Charset) settings.get("client.encoding"));
		this.dateTimeCodec = new DateTimeCodec(DateFormat.getDateInstance(),TimeZone.getDefault());
		this.in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
		this.out = new DataOutputStream(socket.getOutputStream());
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

	public void refreshType(int typeId) {
	}
	
	public void init() throws IOException {
		
		if(start()) {
			
			//Load types
			String typeSQL = PgType.INSTANCE.getSQL(serverVersion);
			List<PgType.Row> pgTypes = query(typeSQL, PgType.Row.class);
			
			//Load attributes
			String attrsSQL = PgAttribute.INSTANCE.getSQL(serverVersion);
			List<PgAttribute.Row> pgAttrs = query(attrsSQL, PgAttribute.Row.class);
			
			//Load procs
			String procsSQL = PgProc.INSTANCE.getSQL(serverVersion);
			List<PgProc.Row> pgProcs = query(procsSQL, PgProc.Row.class);
			
			//Update the registry with known types
			Registry.update(pgTypes, pgAttrs, pgProcs);
		}
	}
	
	private boolean start() throws IOException {
		
		Map<String, Object> params = new HashMap<String, Object>();

		params.put("application_name", "pgjdbc app");
		params.put("client_encoding", "UTF8");
		params.put("database", settings.get("database"));
		params.put("user", settings.get("username"));
		
		StartupProtocol startupProto = new StartupProtocol(this);
		
		startupProto.startup(params);
		
		startupProto.run();
		
		return startupProto.getError() == null;
	}
	
	public <T> List<T> query(String queryTxt, Class<T> rowType) throws IOException {
		
		QueryProtocol<T> queryProto = QueryProtocol.get(this, rowType);
		
		queryProto.queryParse(null, queryTxt, Collections.<Type>emptyList());
		
		queryProto.queryBind(null, null, Collections.<Object>emptyList());

		queryProto.describe('P', null);

		queryProto.queryExecute(null, 0);
		
		queryProto.flush();
		
		queryProto.sync();

		queryProto.run();
		
		return queryProto.getResults();
	}

	@Override
	public void setKeyData(int processId, int secretKey) {

		keyData = new KeyData();
		keyData.processId = processId;
		keyData.secretKey = secretKey;
	}

	@Override
	public TupleType createTupleType(List<Field> fields) {
		
		TupleType tupleType = new TupleType(-1, "", null, 0);
		
		List<Attribute> attrs = new ArrayList<Attribute>();
		
		for(Field field : fields) {
			
			Attribute attr = new Attribute();
			
			attr.name = field.name;
			attr.type = loadType(field.typeId);
			
			attrs.add(attr);
		}
		
		tupleType.setAttributes(attrs);
		
		return tupleType;
	}

	@Override
	public void restart(TransactionStatus txStatus) {
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
			
		case "integer_timestamps":

			//TODO: set timestamp format
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
