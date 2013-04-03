package com.impossibl.postgres.datetime;

public class FormatUtils {

	static void checkOffset(String value, int offset, char expected) throws IndexOutOfBoundsException {
		if(offset < 0) {
			throw new IndexOutOfBoundsException("Not enough characters");
		}
		if(expected == '\0')
			return;
		char found = value.charAt(offset);
		if(found != expected) {
			throw new IndexOutOfBoundsException("Expected '" + expected + "' character but found '" + found + "'");
		}
	}

	static int parseInt(String value, int start, int[] res) {
		
		int i = start, end = value.length();
		int result = 0;
		int digit;
		if(i < end) {
			digit = Character.digit(value.charAt(i), 10);
			if(digit < 0) {
				return ~i;
			}
			i++;
			result = -digit;
		}
		while(i < end) {
			digit = Character.digit(value.charAt(i), 10);
			if(digit < 0) {
				break;
			}
			i++;
			result *= 10;
			result -= digit;
		}
		res[0] = -result;
		return i;
	}

}
