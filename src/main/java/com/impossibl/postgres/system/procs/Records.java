package com.impossibl.postgres.system.procs;

import static com.impossibl.postgres.types.PrimitiveType.Record;
import static com.impossibl.postgres.utils.Factory.createInstance;
import static org.apache.commons.beanutils.BeanUtils.getProperty;
import static org.apache.commons.beanutils.BeanUtils.setProperty;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.jboss.netty.buffer.ChannelBuffer;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.CompositeType;
import com.impossibl.postgres.types.CompositeType.Attribute;
import com.impossibl.postgres.types.PrimitiveType;
import com.impossibl.postgres.types.Type;



public class Records extends SimpleProcProvider {

	public Records() {
		super(null, null, new Encoder(), new Decoder(), "record_");
	}

	static class Decoder implements Type.Codec.Decoder {

		public PrimitiveType getInputPrimitiveType() {
			return Record;
		}
		
		public Class<?> getOutputType() {
			return Object.class;
		}

		public Object decode(Type type, ChannelBuffer buffer, Context context) throws IOException {

			CompositeType ctype = (CompositeType) type;

			Object instance = createInstance(context.lookupInstanceType(type), 0);

			int lengthGiven = buffer.readInt();

			long readStart = buffer.readerIndex();

			int itemCount = buffer.readInt();

			for (int c = 0; c < itemCount; ++c) {

				Attribute attribute = ctype.getAttribute(c+1);

				Type attributeType = context.getRegistry().loadType(buffer.readInt());

				if (attributeType.getId() != attribute.type.getId()) {

					context.refreshType(attributeType.getId());
				}

				Object attributeVal = attributeType.getBinaryCodec().decoder.decode(attributeType, buffer, context);

				Records.set(instance, attribute.name, attributeVal);
			}

			if (lengthGiven != buffer.readerIndex() - readStart) {
				throw new IllegalStateException();
			}

			return instance;
		}

	}

	static class Encoder implements Type.Codec.Encoder {

		public Class<?> getInputType() {
			return Object.class;
		}

		public PrimitiveType getOutputPrimitiveType() {
			return Record;
		}
		
		public void encode(Type type, ChannelBuffer buffer, Object val, Context context) throws IOException {

			if (val == null) {

				buffer.writeInt(-1);
			}
			else {
				
				buffer.writeInt(-1);

				int writeStart = buffer.writerIndex();

				CompositeType ctype = (CompositeType) type;
				
				Collection<Attribute> attributes = ctype.getAttributes();

				buffer.writeInt(attributes.size());

				for (Attribute attribute : attributes) {

					Type attributeType = attribute.type;

					buffer.writeInt(attributeType.getId());

					Object attributeVal = Records.get(val, attribute.name);

					attributeType.getBinaryCodec().encoder.encode(attributeType, buffer, attributeVal, context);
				}

				//Set length
				buffer.setInt(writeStart-4, buffer.writerIndex() - writeStart);
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
