package com.impossibl.postgres.system.procs;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.impossibl.postgres.Context;
import com.impossibl.postgres.types.Array;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.DataInputStream;
import com.impossibl.postgres.utils.DataOutputStream;


public class Arrays extends SimpleProcProvider {

	public Arrays() {
		super(null, null, new Receive(), new Send(), "array_");
	}
	
	static class Send implements Type.BinaryIO.SendHandler {

		public Object handle(Type type, DataInputStream stream, Context context) throws IOException {
			
			Array atype = (Array)type;
			Type elementType = atype.getElementType();
			
			Object instance = context.createInstance(context.lookupInstanceType(type));
			
			int len = stream.readInt();
			
			instance = Arrays.resize(instance, len);
			
			for(int c=0; c < len; ++c) {

				Object elementVal = elementType.getBinaryIO().send.handle(elementType, stream, context);
				
				Arrays.set(instance, c, elementVal);
			}
			
			return instance;
		}

	}

	static class Receive implements Type.BinaryIO.ReceiveHandler {

		public void handle(Type type, DataOutputStream stream, Object val, Context context) throws IOException {
			stream.writeBoolean((Boolean)val);
		}

	}

	private static Object resize(Object val, int len) {
		
		if(val.getClass().isArray()) {
			return java.lang.reflect.Array.newInstance(val.getClass().getComponentType(), len);
		}
		
		return val;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void set(Object val, int c, Object elementVal) {

		if(val.getClass().isArray()) {
			java.lang.reflect.Array.set(val, c, elementVal);
		}
		else if(val instanceof List) {
			((List)val).add(elementVal);
		}
		else if(val instanceof Map) {
			((Map)val).put(c, elementVal);
		}
	}

}
