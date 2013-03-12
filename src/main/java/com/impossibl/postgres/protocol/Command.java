package com.impossibl.postgres.protocol;

import java.util.List;

public interface Command {
	
	Notice getError();
	List<Notice> getWarnings();

}
