package com.impossibl.postgres.system;

import com.impossibl.postgres.protocol.Notice;

public class NoticeException extends Exception {
	
	private static final long serialVersionUID = 7459731038298685932L;
	
	
	private Notice notice;

	public NoticeException(String message, Notice notice) {
		super(message);
		this.notice = notice;
	}

	public Notice getNotice() {
		return notice;
	}

	public void setNotice(Notice notice) {
		this.notice = notice;
	}
	
}
