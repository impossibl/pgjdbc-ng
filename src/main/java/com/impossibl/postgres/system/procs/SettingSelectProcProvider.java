package com.impossibl.postgres.system.procs;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.Type.Codec;

public class SettingSelectProcProvider extends BaseProcProvider {

	String settingName;
	Object settingMatchValue;
	Codec.Encoder matchedTxtEncoder;
	Codec.Decoder matchedTxtDecoder;
	Codec.Encoder matchedBinEncoder;
	Codec.Decoder matchedBinDecoder;
	Codec.Encoder unmatchedTxtEncoder;
	Codec.Decoder unmatchedTxtDecoder;
	Codec.Encoder unmatchedBinEncoder;
	Codec.Decoder unmatchedBinDecoder;
	
	public SettingSelectProcProvider(
			String settingName, Object settingMatchValue,
			Codec.Encoder matchedTxtEncoder, Codec.Decoder matchedTxtDecoder, Codec.Encoder matchedBinEncoder, Codec.Decoder matchedBinDecoder, 
			Codec.Encoder unmatchedTxtEncoder, Codec.Decoder unmatchedTxtDecoder, Codec.Encoder unmatchedBinEncoder, Codec.Decoder unmatchedBinDecoder, 
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

	public Codec.Encoder findEncoder(String name, Context context) {
		if(name.endsWith("recv") && hasName(name, "recv")) {
			if(context != null && settingMatchValue.equals(context.getSetting(settingName)))
				return matchedBinEncoder;
			else
				return unmatchedBinEncoder;
		}
		else if(name.endsWith("in") && hasName(name, "in")) {
			if(context != null && settingMatchValue.equals(context.getSetting(settingName)))
				return matchedTxtEncoder;
			else
				return unmatchedTxtEncoder;
		}
		return null;
	}

	public Codec.Decoder findDecoder(String name, Context context) {
		if(name.endsWith("send") && hasName(name, "send")) {
			if(context != null && settingMatchValue.equals(context.getSetting(settingName)))
				return matchedBinDecoder;
			else
				return unmatchedBinDecoder;
		}
		else if(name.endsWith("out") && hasName(name, "out")) {
			if(context != null && settingMatchValue.equals(context.getSetting(settingName)))
				return matchedTxtDecoder;
			else
				return unmatchedTxtDecoder;
		}
		return null;
	}
	
}
