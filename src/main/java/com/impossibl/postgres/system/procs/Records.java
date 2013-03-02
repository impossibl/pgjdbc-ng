package com.impossibl.postgres.system.procs;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.commons.beanutils.BeanMap;

import com.impossibl.postgres.Context;
import com.impossibl.postgres.types.Composite;
import com.impossibl.postgres.types.Composite.Attribute;
import com.impossibl.postgres.types.Registry;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.DataInputStream;
import com.impossibl.postgres.utils.DataOutputStream;


public class Records extends SimpleProcProvider {

	public Records() {
		super(null, null, new Receive(), new Send(), "record_");
	}
	
	static class Send implements Type.BinaryIO.SendHandler {

		public Object handle(Type type, DataInputStream stream, Context context) throws IOException {
			
			Composite ctype = (Composite) type;
			
			Object instance = context.createInstance(context.lookupInstanceType(type));
			
			Map<Object, Object> target = makeMap(instance);
			
			int lengthGiven = stream.readInt();
			
			long writeStart = stream.getCount();
			
			int itemCount = stream.readInt();
			for(int c=0; c < itemCount; ++c) {
				
				Attribute attribute = ctype.getAttribute(c);
				
				Type attributeType = Registry.loadType(stream.readInt());
				
				if(attributeType.getId() != attribute.type.getId()) {

					context.refreshType(attributeType.getId());
				}
				
				Object attributeVal = null;
				
				stream.mark(4);
				if(stream.readInt() != -1) {
				
					stream.reset();

					attributeVal = attributeType.getBinaryIO().send.handle(attributeType, stream, context);
				}
				
				target.put(attribute.name, attributeVal);
			}
			
			long lengthFound = stream.getCount() - writeStart;
			if(lengthFound != lengthGiven) {
				throw new IllegalStateException();
			}
			
			return instance;
		}

	}

	static class Receive implements Type.BinaryIO.ReceiveHandler {

		public void handle(Type type, DataOutputStream stream, Object val, Context context) throws IOException {
			
			Composite ctype = (Composite) type;
			
			Map<Object, Object> source = makeMap(val);
			
			Collection<Attribute> attributes = ctype.getAttributes();
			
			stream.writeInt(attributes.size());
			
			for(Attribute attribute : attributes) {
				
				Type attributeType = attribute.type;
				
				stream.writeInt(attributeType.getId());
				
				Object attributeVal = source.get(attribute.name);
				
				attributeType.getBinaryIO().recv.handle(attributeType, stream, attributeVal, context);
			}
			
		}

	}

	@SuppressWarnings("unchecked")
	public static Map<Object, Object> makeMap(Object instance) {

		if(instance instanceof Map) {
			return (Map<Object, Object>) instance;
		}
		
		return new BeanMap(instance);
	}

}
