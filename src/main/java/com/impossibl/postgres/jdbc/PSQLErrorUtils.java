package com.impossibl.postgres.jdbc;

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.Iterator;
import java.util.List;

import com.impossibl.postgres.protocol.Notice;

public class PSQLErrorUtils {
	
	public static SQLWarning makeSQLWarningChain(List<Notice> notices) {
		
		Iterator<Notice> noticeIter = notices.iterator();
		
		SQLWarning root = null;
		
		if(noticeIter.hasNext()) {
		
			root = makeSQLWarning(noticeIter.next());
			SQLWarning current = root;
			
			while(noticeIter.hasNext()) {
				
				Notice notice = noticeIter.next();

				//Only include warnings...
				if(!notice.isWarning())
					continue;
				
				SQLWarning nextWarning = makeSQLWarning(notice);
				current.setNextWarning(nextWarning);
				current = nextWarning;
			}
		
		}
		
		return root;
	}
	
	public static SQLException makeSQLExceptionChain(List<Notice> notices) {
	
		Iterator<Notice> noticeIter = notices.iterator();
		
		SQLException root = null;
		
		if(noticeIter.hasNext()) {
		
			root = makeSQLException(noticeIter.next());
			SQLException current = root;
			
			while(noticeIter.hasNext()) {
				
				SQLException nextException = makeSQLException(noticeIter.next());
				current.setNextException(nextException);
				current = nextException;
			}
		
		}
		
		return root;
	}
	
	public static SQLWarning makeSQLWarning(Notice notice) {
		
		if(notice.isWarning()) {
			throw new IllegalArgumentException("notice not an error");
			
		}
		
		return new SQLWarning(notice.message, notice.code);
	}

	public static SQLException makeSQLException(Notice notice) {
		
		return new SQLException(notice.message, notice.code);
	}

}
