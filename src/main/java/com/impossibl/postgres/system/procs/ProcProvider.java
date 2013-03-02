package com.impossibl.postgres.system.procs;

import com.impossibl.postgres.types.Type.BinaryIO;
import com.impossibl.postgres.types.Type.TextIO;

public interface ProcProvider {
	
	public BinaryIO.ReceiveHandler findBinaryReceiveHandler(String name);
	public BinaryIO.SendHandler findBinarySendHandler(String name);
	public TextIO.InputHandler findTextInputHandler(String name);
	public TextIO.OutputHandler findTextOutputHandler(String name);

}
