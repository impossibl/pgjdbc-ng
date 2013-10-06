/**
 * Copyright (c) 2013, impossibl.com
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  * Neither the name of impossibl.com nor the names of its contributors may
 *    be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package com.impossibl.postgres.system.procs;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.Modifiers;
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
		if(name.endsWith("recv") && hasName(name, "recv", context)) {
			if(context != null && settingMatchValue.equals(context.getSetting(settingName)))
				return matchedBinEncoder;
			else
				return unmatchedBinEncoder;
		}
		else if(name.endsWith("in") && hasName(name, "in", context)) {
			if(context != null && settingMatchValue.equals(context.getSetting(settingName)))
				return matchedTxtEncoder;
			else
				return unmatchedTxtEncoder;
		}
		return null;
	}

	public Codec.Decoder findDecoder(String name, Context context) {
		if(name.endsWith("send") && hasName(name, "send", context)) {
			if(context != null && settingMatchValue.equals(context.getSetting(settingName)))
				return matchedBinDecoder;
			else
				return unmatchedBinDecoder;
		}
		else if(name.endsWith("out") && hasName(name, "out", context)) {
			if(context != null && settingMatchValue.equals(context.getSetting(settingName)))
				return matchedTxtDecoder;
			else
				return unmatchedTxtDecoder;
		}
		return null;
	}

	@Override
	public Modifiers.Parser findModifierParser(String name, Context context) {
		return null;
	}
	
}
