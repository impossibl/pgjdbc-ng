package com.impossibl.postgres;

import static com.impossibl.postgres.BasicContext.State.Error;
import static com.impossibl.postgres.BasicContext.State.Ready;
import static com.impossibl.postgres.BasicContext.State.Start;
import static com.impossibl.postgres.types.Registry.loadType;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
import com.impossibl.postgres.protocol.Field;
import com.impossibl.postgres.protocol.Protocol;
import com.impossibl.postgres.protocol.Protocol30;
import com.impossibl.postgres.protocol.TransactionStatus;
import com.impossibl.postgres.types.Composite.Attribute;
import com.impossibl.postgres.types.Tuple;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.DataInputStream;
import com.impossibl.postgres.utils.DataOutputStream;

public class BasicContext implements Context {
	
	private static final Logger logger = Logger.getLogger(BasicContext.class.getName());
	
	
	public enum State {
		Start,
		Ready,
		Error, Query
	}
	
	public static class KeyData {
		int processId;
		int secretKey;
	}
	
	
	private State state;
	private Map<String, Class<?>>  targetTypeMap;
	private StringCodec stringCodec;
	private DateTimeCodec dateTimeCodec;
	private Protocol protocol;
	private Map<String, Object> settings;
	private KeyData keyData;
	private Tuple resultType;
	
	
	public BasicContext(InputStream in, OutputStream out, Map<String, Object> settings) {
		this.targetTypeMap = new HashMap<String, Class<?>>();
		this.settings = new HashMap<String, Object>(settings);
		this.stringCodec = new StringCodec((Charset) settings.get("client.encoding"));
		this.dateTimeCodec = new DateTimeCodec(
				DateFormat.getDateInstance(),
				TimeZone.getDefault());
		this.protocol = new Protocol30(this, new DataInputStream(new BufferedInputStream(in)), new DataOutputStream(out));
		this.state = Start;
	}

	public Protocol getProtocol() {
		return protocol;
	}

	@Override
	public Object getSetting(String name) {
		return settings.get(name);
	}

	public Class<?> lookupInstanceType(Type type) {
		
		return targetTypeMap.get(type.getName());
	}
	
	public Object createInstance(Class<?> type) {
		
		if(type == null)
			return new HashMap<String, Object>();
		
		try {
			return type.newInstance();
		}
		catch (InstantiationException e) {
			throw new RuntimeException(e);
		}
		catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		}
		
	}

	public StringCodec getStringCodec() {
		return stringCodec;
	}

	public void refreshType(int typeId) {
	}
	
	public boolean start() throws IOException {
		
		Map<String, Object> params = new HashMap<String, Object>();
		
		params.put("database", settings.get("database"));
		params.put("user", settings.get("username"));
		
		protocol.startup(params);
		
		while(state != Ready) {
			protocol.dispatch(this);
		}
		
		return state != Error;
	}
	
	public boolean query(String query) throws IOException {
		
		state = State.Query;
		
		protocol.queryParse(null, query, Collections.<Type>emptyList());
		
		protocol.queryBind(null, null, Collections.<Type>emptyList(), Collections.<Object>emptyList());

		protocol.describe('P', null);

		protocol.queryExecute(null, 0);
		
		while(state != Ready) {
			protocol.dispatch(this);
		}
		
		return state != Error;
	}

	@Override
	public void setKeyData(int processId, int secretKey) {

		keyData = new KeyData();
		keyData.processId = processId;
		keyData.secretKey = secretKey;
	}

	@Override
	public Tuple createTupleType(List<Field> fields) {
		
		Tuple tupleType = new Tuple(-1, "", null, 0);
		
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
		state = State.Ready;
	}

	@Override
	public void setParameterDescriptions(List<Type> asList) {
	}

	@Override
	public Type getParameterDataType() {
		return null;
	}

	@Override
	public void setResultType(Tuple type) {
		resultType = type;
	}

	@Override
	public Tuple getResultType() {
		return resultType;
	}

	@Override
	public void setResultData(Object value) {
	}

	@Override
	public void commandComplete(String commandTag) {
	}

	@Override
	public void bindComplete() {
	}

	@Override
	public void closeComplete() {
	}

	@Override
	public void updateSystemParameter(String name, String value) {
		
		logger.info("system paramter: " + name + "=" + value);
		
		switch(name) {
		
		case "server_version":
			
			//setServerVersion(value);
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
	public void reportError(byte type, String value) {
		logger.severe(value);
	}

	@Override
	public void authenticated() {
	}

}
