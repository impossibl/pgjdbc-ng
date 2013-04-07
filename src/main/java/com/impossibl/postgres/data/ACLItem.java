package com.impossibl.postgres.data;

import static com.google.common.base.Strings.isNullOrEmpty;

import java.util.regex.Matcher;
import java.util.regex.Pattern;



public class ACLItem {

	public String user;
	public String privileges;
	public String grantor;

	public ACLItem(String user, String privileges, String grantor) {
		super();
		this.user = user;
		this.privileges = privileges;
		this.grantor = grantor;
	}

	private ACLItem() {
	}

	@Override
	public String toString() {
		
		StringBuilder sb = new StringBuilder();
		
		if(user != null && !user.equals("PUBLIC")) {
			sb.append(user);
		}
		
		sb.append('=');
		
		if(privileges != null) {
			sb.append(privileges);
		}
		
		sb.append('/');
		
		if(grantor != null) {
			sb.append(grantor);
		}
		
		return sb.toString();
	}

	private static final Pattern ACL_PATTERN = Pattern.compile("(.*)=(\\w*)/(.*)");

	public static ACLItem parse(String aclItemStr) {

		ACLItem aclItem = null;

		Matcher aclMatcher = ACL_PATTERN.matcher(aclItemStr);
		if(aclMatcher.matches()) {

			aclItem = new ACLItem();

			aclItem.user = aclMatcher.group(1);
			if(isNullOrEmpty(aclItem.user)) {
				aclItem.user = "PUBLIC";
			}

			aclItem.privileges = aclMatcher.group(2);
			aclItem.grantor = aclMatcher.group(3);

		}

		return aclItem;
	}

}
