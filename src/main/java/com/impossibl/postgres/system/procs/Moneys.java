package com.impossibl.postgres.system.procs;

import static java.math.RoundingMode.HALF_UP;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.DataInputStream;
import com.impossibl.postgres.utils.DataOutputStream;


public class Moneys extends SimpleProcProvider {

	public Moneys() {
		super(null, null, new Encoder(), new Decoder(), "cash_");
	}

	static class Decoder implements Type.BinaryIO.Decoder {

		public BigDecimal decode(Type type, DataInputStream stream, Context context) throws IOException {

			int length = stream.readInt();
			if (length == -1) {
				return null;
			}
			else if (length != 8) {
				throw new IOException("invalid length");
			}

			long val = stream.readLong();
			
			int fracDigits = getFractionalDigits(context);
			
			return new BigDecimal(BigInteger.valueOf(val), fracDigits);
		}

	}

	static class Encoder implements Type.BinaryIO.Encoder {

		public void encode(Type type, DataOutputStream stream, Object val, Context context) throws IOException {

			if (val == null) {
				
				stream.writeInt(-1);
			}
			else {
				
				int fracDigits = getFractionalDigits(context);
				
				BigDecimal dec = (BigDecimal) val;
				
				dec.setScale(fracDigits, HALF_UP);
				
				stream.writeInt(8);
				stream.writeLong(dec.unscaledValue().longValue());
			}
			
		}

	}

	static int getFractionalDigits(Context context) {
		
		Object val = context.getSetting("money.fractionalDigits");
		if(val == null)
			return 2;
		
		return (int)(Integer)val;
	}

}
