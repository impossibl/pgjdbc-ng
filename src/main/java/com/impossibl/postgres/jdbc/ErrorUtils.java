package com.impossibl.postgres.jdbc;

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.Iterator;
import java.util.List;

import com.impossibl.postgres.protocol.Notice;


/**
 * Utilities for creating SQLException and SQLWarnings from PostgreSQL's
 * "Notice" and "Error" message data
 * 
 * @author kdubb
 *
 */
public class ErrorUtils {

	/**
	 * Converts the given list of notices into a chained list of SQLWarnings
	 * 
	 * @param notices List of notices to convert
	 * @return Root of converted list or null is no notices were given
	 */
	public static SQLWarning makeSQLWarningChain(List<Notice> notices) {

		Iterator<Notice> noticeIter = notices.iterator();

		SQLWarning root = null;

		if(noticeIter.hasNext()) {

			root = makeSQLWarning(noticeIter.next());
			SQLWarning current = root;

			while(noticeIter.hasNext()) {

				Notice notice = noticeIter.next();

				// Only include warnings...
				if(!notice.isWarning())
					continue;

				SQLWarning nextWarning = makeSQLWarning(notice);
				current.setNextWarning(nextWarning);
				current = nextWarning;
			}

		}

		return root;
	}

	/**
	 * Converts the given list of notices into a chained list of SQLExceptions
	 * 
	 * @param notices List of notices to convert
	 * @return Root of converted list or null is no notices were given
	 */
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

	/**
	 * Converts a single warning notice to a single SQLWarning
	 * 
	 * @param notice Notice to convert
	 * @return SQLWarning
	 */
	public static SQLWarning makeSQLWarning(Notice notice) {

		if(notice.isWarning()) {
			throw new IllegalArgumentException("notice not an error");

		}

		return new SQLWarning(notice.getMessage(), notice.getCode());
	}

	/**
	 * Converts a single error notice to a single SQLException
	 * 
	 * @param notice Notice to convert
	 * @return SQLException
	 */
	public static SQLException makeSQLException(Notice notice) {

		return new SQLException(notice.getMessage(), notice.getCode());
	}

	/**
	 * Creates a single chain of warnings. The method attempts to append to the
	 * base chain but if no base is provided it returns the add chain.
	 *  
	 * @param base Base warning chain
	 * @param add Warning chain to append to base
	 * @return Head of the new complete warning chain
	 */
	public static SQLWarning chainWarnings(SQLWarning base, SQLWarning add) {
		
		if(base == null)
			return add;

		SQLWarning current = base;
		while(current.getNextWarning() != null) {
			current = current.getNextWarning();
		}
		
		current.setNextWarning(add);
		
		return base;
	}
 
}
