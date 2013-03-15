package com.impossibl.postgres.system.procs;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Types;

import org.jboss.netty.buffer.ChannelBuffer;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.Type;



public class Numerics extends SimpleProcProvider {

	private static final short NUMERIC_POS = 0x0000;
	private static final short NUMERIC_NEG = 0x4000;
	private static final short DEC_DIGITS = 4;

	public Numerics() {
		super(null, null, new Encoder(), new Decoder(), "numeric_");
	}

	static class Decoder implements Type.Codec.Decoder {

		public int getInputSQLType() {
			return Types.NUMERIC;
		}
		
		public Class<?> getOutputType() {
			return BigDecimal.class;
		}

		public BigDecimal decode(Type type, ChannelBuffer buffer, Context context) throws IOException {

			int length = buffer.readInt();
			if (length == -1) {
				return null;
			}
			else if (length < 8) {
				throw new IOException("invalid length");
			}

			short digitCount = buffer.readShort();

			short[] info = new short[3];
			info[0] = buffer.readShort();	//weight
			info[1] = buffer.readShort();	//sign
			info[2] = buffer.readShort();	//displayScale

			short[] digits = new short[digitCount];
			for (int d = 0; d < digits.length; ++d)
				digits[d] = buffer.readShort();

			String num = decodeToString(info[0], info[1], info[2], digits);

			return new BigDecimal(num);
		}

	}

	static class Encoder implements Type.Codec.Encoder {

		public Class<?> getInputType() {
			return BigDecimal.class;
		}

		public int getOutputSQLType() {
			return Types.NUMERIC;
		}
		
		public void encode(Type type, ChannelBuffer buffer, Object val, Context context) throws IOException {
			if (val == null) {

				buffer.writeInt(-1);
			}
			else {

				String num = ((BigDecimal) val).toPlainString();

				short[] info = new short[3];
				short[] digits = encodeFromString(num, info);

				buffer.writeShort(info[0]);	//weight
				buffer.writeShort(info[1]);	//sign
				buffer.writeShort(info[2]);	//displayScale

				for (int d = 0; d < digits.length; ++d)
					buffer.writeShort(digits[d]);
			}

		}

	}

	/**
	 * Encodes a string of the plain form XXXX.XXX into an NBASE packed sequence
	 * of shorts.
	 * 
	 * @param num
	 * @param info
	 * @return
	 */
	private static short[] encodeFromString(String num, short[] info) {

		char[] numChars = num.toCharArray();
		byte[] numDigs = new byte[numChars.length - 1 + DEC_DIGITS * 2];
		int ch = 0;
		int digs = DEC_DIGITS;
		boolean haveDP = false;
		
		//Swallow leading zeros
		while(numChars[ch] == '0') ch++;

		short sign = NUMERIC_POS;
		short displayWeight = -1;
		short displayScale = 0;

		if (numChars[ch] == '-') {
			sign = NUMERIC_NEG;
			++ch;
		}
		
		/*
		 * Copy to array of single byte digits
		 */

		while (ch < numChars.length) {

			if (numChars[ch] == '.') {

				haveDP = true;
				ch++;
			}
			else {

				numDigs[digs++] = (byte) (numChars[ch++] - '0');
				if (!haveDP)
					displayWeight++;
				else
					displayScale++;
			}

		}
		
		digs -= DEC_DIGITS;

		/*
		 * Pack into NBASE format
		 */

		short weight;

		if (displayWeight >= 0)
			weight = (short) ((displayWeight + 1 + DEC_DIGITS - 1) / DEC_DIGITS - 1);
		else
			weight = (short) -((-displayWeight - 1) / DEC_DIGITS + 1);

		int offset = (weight + 1) * DEC_DIGITS - (displayWeight + 1);
		int digitCount = (digs + offset + DEC_DIGITS - 1) / DEC_DIGITS;

		int i = DEC_DIGITS - offset;
		short[] digits = new short[digitCount];
		int d = 0;

		while (digitCount-- > 0) {
			digits[d++] = (short) (((numDigs[i] * 10 + numDigs[i + 1]) * 10 + numDigs[i + 2]) * 10 + numDigs[i + 3]);
			i += DEC_DIGITS;
		}

		info[0] = weight;
		info[1] = sign;
		info[2] = displayScale;
		return digits;
	}

	/**
	 * Decodes a sequence of digits NBASE packed in shorts into a string of the
	 * plain form XXXX.XXX
	 * 
	 * @param weight
	 * @param sign
	 * @param displayScale
	 * @param digits
	 * @return String representation of the decimal number
	 */
	private static String decodeToString(short weight, short sign, short displayScale, short[] digits) {

		StringBuilder sb = new StringBuilder();

		if (sign == NUMERIC_NEG) {
			sb.append('-');
		}

		/*
		 * Digits before decimal
		 */
		int d = 0;

		if (weight < 0) {
			d = weight + 1;
			sb.append(0);
		}
		else {

			for (d = 0; d <= weight; d++) {

				short dig = d < digits.length ? digits[d] : 0;
				boolean putIt = (d > 0);
				
				for (int b = 1000; b > 1; b /= 10) {
					
					short d1 = (short) (dig / b);
					dig -= d1 * b;
					putIt |= (d1 > 0);
					if(putIt)
						sb.append((char) (d1 + '0'));
				}

				sb.append((char) (dig + '0'));

			}

		}

		/*
		 * Digits after decimal
		 */

		if (displayScale > 0) {

			sb.append('.');

			int length = sb.length() + displayScale;
			
			for (int i = 0; i < displayScale; d++, i += DEC_DIGITS) {

				short dig = (d >= 0 && d < digits.length) ? digits[d] : 0;

				for (int b = 1000; b > 1 && sb.length() < length; b /= 10) {

					short d1 = (short) (dig / b);
					dig -= d1 * b;
					sb.append((char) (d1 + '0'));
				}

				if(sb.length() < length)
					sb.append((char) (dig + '0'));

			}

		}
		return sb.toString();
	}

}
