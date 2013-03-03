package com.impossibl.postgres.protocol;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.beanutils.BeanMap;

import com.impossibl.postgres.Context;
import com.impossibl.postgres.types.Composite.Attribute;
import com.impossibl.postgres.types.Tuple;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.DataInputStream;

public class DataRowMP implements MessageProcessor {

	@Override
	public void process(DataInputStream in, Context context) throws IOException {

		Tuple tupleType = context.getResultType();

		Object instance = context.createInstance(context.lookupInstanceType(tupleType));

		Map<Object, Object> target = makeMap(instance);

		int itemCount = in.readShort();
		
		for (int c = 0; c < itemCount; ++c) {

			Attribute attribute = tupleType.getAttribute(c);

			Type attributeType = attribute.type;
			Object attributeVal = null;
			
			in.mark(4);
			if (in.readInt() != -1) {

				attributeVal = attributeType.getBinaryIO().decoder.decode(attributeType, in, context);
			}

			target.put(attribute.name, attributeVal);
		}

		context.setResultData(instance);		
	}

	@SuppressWarnings("unchecked")
	public static Map<Object, Object> makeMap(Object instance) {

		if (instance instanceof Map) {
			return (Map<Object, Object>) instance;
		}

		return new BeanMap(instance);
	}

}
