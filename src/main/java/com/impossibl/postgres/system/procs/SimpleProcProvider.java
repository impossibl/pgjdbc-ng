package com.impossibl.postgres.system.procs;

import com.impossibl.postgres.types.Type.BinaryIO;
import com.impossibl.postgres.types.Type.BinaryIO.ReceiveHandler;
import com.impossibl.postgres.types.Type.BinaryIO.SendHandler;
import com.impossibl.postgres.types.Type.TextIO;
import com.impossibl.postgres.types.Type.TextIO.InputHandler;
import com.impossibl.postgres.types.Type.TextIO.OutputHandler;

public class SimpleProcProvider implements ProcProvider {

	String[] baseNames;
	TextIO.InputHandler input;
	TextIO.OutputHandler output;
	BinaryIO.ReceiveHandler recv;
	BinaryIO.SendHandler send;
	
	public SimpleProcProvider(InputHandler input, OutputHandler output, ReceiveHandler recv, SendHandler send, String... baseNames) {
		super();
		this.baseNames = baseNames;
		this.input = input;
		this.output = output;
		this.recv = recv;
		this.send = send;
	}

	public ReceiveHandler findBinaryReceiveHandler(String name) {
		if(hasName(name, "recv"))
			return recv;
		return null;
	}

	public SendHandler findBinarySendHandler(String name) {
		if(hasName(name, "send"))
			return send;
		return null;
	}

	public InputHandler findTextInputHandler(String name) {
		if(hasName(name, "in"))
			return input;
		return null;
	}

	public OutputHandler findTextOutputHandler(String name) {
		if(hasName(name, "out"))
			return output;
		return null;
	}
	
	private boolean hasName(String name, String suffix) {
		
		for(String baseName : baseNames) {
			if(name.equals(baseName+suffix))
				return true;
		}
		
		return false;
	}

}
