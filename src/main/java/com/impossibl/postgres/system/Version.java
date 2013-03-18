package com.impossibl.postgres.system;

import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Version {
	
	//private static final Pattern VERSION_PATTERN = compile("(\\d+)(?:\\.(\\d+)(?:\\.(\\d+))?)?");
	private static final Pattern VERSION_PATTERN = Pattern.compile("(\\d+)(?:\\.(\\d+))?(?:\\.(\\d+))?");
	private static final HashMap<Version,Version> all = new HashMap<Version,Version>();
	
	int major;
	Integer minor;
	Integer revision;
	
	public static Version parse(String versionString) {
		
		Matcher matcher = VERSION_PATTERN.matcher(versionString);
		if(!matcher.find())
			return null;
		
		int major = Integer.parseInt(matcher.group(1));
		Integer minor = matcher.group(2) != null ? Integer.parseInt(matcher.group(2)) : null;
		Integer revision = matcher.group(3) != null ? Integer.parseInt(matcher.group(3)) : null;
		
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
		return this.major == major && (minor == null || minor == this.minor) && (revision == null || revision == this.revision);
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(major);
		if(minor != null)
			sb.append( "." + minor);
		if(revision != null)
			sb.append("." + revision);
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
