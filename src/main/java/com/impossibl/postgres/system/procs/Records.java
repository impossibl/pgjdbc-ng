package com.impossibl.postgres.system.procs;

import static com.impossibl.postgres.utils.Factory.createInstance;
import static org.apache.commons.beanutils.BeanUtils.getProperty;
import static org.apache.commons.beanutils.BeanUtils.setProperty;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.CompositeType;
import com.impossibl.postgres.types.CompositeType.Attribute;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.DataInputStream;
import com.impossibl.postgres.utils.DataOutputStream;



public class Records extends SimpleProcProvider {

	public Records() {
		super(null, null, new Encoder(), new Decoder(), "record_");
	}

	static class Decoder implements Type.BinaryIO.Decoder {

		public Object decode(Type type, DataInputStream stream, Context context) throws IOException {

			CompositeType ctype = (CompositeType) type;

			Object instance = createInstance(context.lookupInstanceType(type), 0);

			int lengthGiven = stream.readInt();

			long writeStart = stream.getCount();

			int itemCount = stream.readInt();

			for (int c = 0; c < itemCount; ++c) {

				Attribute attribute = ctype.getAttribute(c);

				Type attributeType = context.getRegistry().loadType(stream.readInt());

				if (attributeType.getId() != attribute.type.getId()) {

					context.refreshType(attributeType.getId());
				}

				Object attributeVal = attributeType.getBinaryIO().decoder.decode(attributeType, stream, context);

				Records.set(instance, attribute.name, attributeVal);
			}

			long lengthFound = stream.getCount() - writeStart;
			if (lengthFound != lengthGiven) {
				throw new IllegalStateException();
			}

			return instance;
		}

	}

	static class Encoder implements Type.BinaryIO.Encoder {

		public void encode(Type type, DataOutputStream stream, Object val, Context context) throws IOException {

			if (val == null) {

				stream.writeInt(-1);
			}
			else {

				//
				//Write to temp buffer
				//
				ByteArrayOutputStream recordByteStream = new ByteArrayOutputStream();
				DataOutputStream recordDataStream = new DataOutputStream(recordByteStream);

				CompositeType ctype = (CompositeType) type;
				
				Collection<Attribute> attributes = ctype.getAttributes();

				recordDataStream.writeInt(attributes.size());

				for (Attribute attribute : attributes) {

					Type attributeType = attribute.type;

					recordDataStream.writeInt(attributeType.getId());

					Object attributeVal = Records.get(val, attribute.name);

					attributeType.getBinaryIO().encoder.encode(attributeType, recordDataStream, attributeVal, context);
				}
				
				//
				//Write temp buffer
				//
				byte[] buffer = recordByteStream.toByteArray();
				stream.writeInt(buffer.length);
				stream.write(buffer);
			}

		}

	}

	@SuppressWarnings("unchecked")
	protected static Object get(Object instance, String name) {

		if (instance instanceof Map) {

			return ((Map<Object, Object>) instance).get(name);
		}
		else {

			try {

				java.lang.reflect.Field field;

				if ((field = instance.getClass().getField(name)) != null) {
					return field.get(instance);
				}

			}
			catch (ReflectiveOperationException | IllegalArgumentException e) {

				try {
					return getProperty(instance, name.toString());
				}
				catch (ReflectiveOperationException e1) {
				}

			}
		}

		throw new IllegalStateException("invalid poperty name/index");
	}

	@SuppressWarnings("unchecked")
	protected static void set(Object instance, String name, Object value) {

		if (instance instanceof Map) {

			((Map<Object, Object>) instance).put(name, value);
			return;
		}
		else {

			try {

				java.lang.reflect.Field field;

				if ((field = instance.getClass().getField(name)) != null) {

					field.set(instance, value);
					return;
				}

			}
			catch (ReflectiveOperationException | IllegalArgumentException e) {

				try {

					setProperty(instance, name.toString(), value);
					return;
				}
				catch (ReflectiveOperationException e1) {
				}

			}
		}

		throw new IllegalStateException("invalid poperty name/index");
	}

}
