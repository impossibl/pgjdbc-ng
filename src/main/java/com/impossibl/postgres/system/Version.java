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
package com.impossibl.postgres.system;

import java.util.HashMap;

public class Version {
	
	private static final HashMap<Version,Version> all = new HashMap<Version,Version>();
	
	private int major;
	private Integer minor;
	private Integer revision;
	
	public static Version parse(String versionString) {
                String[] version = versionString.split("\\.");
		
		int major = Integer.parseInt(version[0]);
		Integer minor = version.length > 1 ? Integer.valueOf(version[1]) : null;
		Integer revision = version.length > 2 ? Integer.valueOf(version[2]) : null;

                return get(major, minor, revision);
	}

	public static synchronized Version get(int major, Integer minor, Integer revision) {
		
		Version test = new Version(major, minor, revision);
		
		Version found = all.get(test);
		if(found == null) {
			
			all.put(test, test);
			found = test;
		}
		
		return found;
	}	
	
	private Version(int major, Integer minor, Integer revision) {

		if(minor == null && revision != null)
			throw new IllegalArgumentException();
		

		this.major = major;
		this.minor = minor;
		this.revision = revision;
	}

	public int getMajor() {
		return major;
	}

	public Integer getMinor() {
		return minor;
	}

	public Integer getRevision() {
		return revision;
	}

	public boolean compatible(Version current) {
		return compatible(current.major, current.minor, current.revision);
	}
	
	public boolean compatible(int major, Integer minor, Integer revision) {
		return this.major >= major 
				&& (minor == null || this.minor == null || minor >= this.minor) 
				&& (revision == null || this.revision == null || revision >= this.revision);
	}

	public boolean equals(Version current) {
		return equals(current.major, current.minor, current.revision);
	}
	
	public boolean equals(int major, Integer minor, Integer revision) {
		return this.major == major && (minor == null || minor.equals(this.minor)) && (revision == null || revision.equals(this.revision));
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(major);
		if(minor != null)
			sb.append('.').append(minor);
		if(revision != null)
			sb.append('.').append(revision);
		return sb.toString();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + major;
		result = prime * result + ((minor == null) ? 0 : minor.hashCode());
		result = prime * result + ((revision == null) ? 0 : revision.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Version other = (Version) obj;
		if (major != other.major)
			return false;
		if (minor == null) {
			if (other.minor != null)
				return false;
		}
		else if (!minor.equals(other.minor))
			return false;
		if (revision == null) {
			if (other.revision != null)
				return false;
		}
		else if (!revision.equals(other.revision))
			return false;
		return true;
	}

}
