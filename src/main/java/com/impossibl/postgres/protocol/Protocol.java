package com.impossibl.postgres.protocol;

import java.io.IOException;

import com.impossibl.postgres.Context;
import com.impossibl.postgres.utils.DataInputStream;

public interface Protocol {

	void dispatch(DataInputStream in, Context context) throws IOException;

	void authenticate(String password) throws IOException;
	
}
