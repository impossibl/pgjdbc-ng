package com.impossibl.postgres.system.procs;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.Type.BinaryIO;
import com.impossibl.postgres.types.Type.TextIO;

public class SettingSelectProcProvider extends BaseProcProvider {

	String settingName;
	Object settingMatchValue;
	TextIO.Encoder matchedTxtEncoder;
	TextIO.Decoder matchedTxtDecoder;
	BinaryIO.Encoder matchedBinEncoder;
	BinaryIO.Decoder matchedBinDecoder;
	TextIO.Encoder unmatchedTxtEncoder;
	TextIO.Decoder unmatchedTxtDecoder;
	BinaryIO.Encoder unmatchedBinEncoder;
	BinaryIO.Decoder unmatchedBinDecoder;
	
	public SettingSelectProcProvider(
			String settingName, Object settingMatchValue,
			TextIO.Encoder matchedTxtEncoder, TextIO.Decoder matchedTxtDecoder, BinaryIO.Encoder matchedBinEncoder, BinaryIO.Decoder matchedBinDecoder, 
			TextIO.Encoder unmatchedTxtEncoder, TextIO.Decoder unmatchedTxtDecoder, BinaryIO.Encoder unmatchedBinEncoder, BinaryIO.Decoder unmatchedBinDecoder, 
			String... baseNames) {
		super(baseNames);
		this.settingName = settingName;
		this.settingMatchValue = settingMatchValue;
		this.matchedTxtEncoder = matchedTxtEncoder;
		this.matchedTxtDecoder = matchedTxtDecoder;
		this.matchedBinEncoder = matchedBinEncoder;
		this.matchedBinDecoder = matchedBinDecoder;
		this.unmatchedTxtEncoder = unmatchedTxtEncoder;
		this.unmatchedTxtDecoder = unmatchedTxtDecoder;
		this.unmatchedBinEncoder = unmatchedBinEncoder;
		this.unmatchedBinDecoder = unmatchedBinDecoder;
	}

	public BinaryIO.Encoder findBinaryEncoder(String name, Context context) {
		if(context != null && hasName(name, "recv")) {
			if(context.getSetting(settingName).equals(settingMatchValue))
				return matchedBinEncoder;
			else
				return unmatchedBinEncoder;
		}
		return null;
	}

	public BinaryIO.Decoder findBinaryDecoder(String name, Context context) {
		if(context != null && hasName(name, "send")) {
			if(context.getSetting(settingName).equals(settingMatchValue))
				return matchedBinDecoder;
			else
				return unmatchedBinDecoder;
		}
		return null;
	}

	public TextIO.Encoder findTextEncoder(String name, Context context) {
		if(context != null && hasName(name, "in")) {
			if(context.getSetting(settingName).equals(settingMatchValue))
				return matchedTxtEncoder;
			else
				return unmatchedTxtEncoder;
		}
		return null;
	}

	public TextIO.Decoder findTextDecoder(String name, Context context) {
		if(context != null && hasName(name, "out")) {
			if(context.getSetting(settingName).equals(settingMatchValue))
				return matchedTxtDecoder;
			else
				return unmatchedTxtDecoder;
		}
		return null;
	}
	
}
