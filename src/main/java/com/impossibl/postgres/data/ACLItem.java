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
