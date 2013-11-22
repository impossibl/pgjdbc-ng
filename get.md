---
layout: default
---
{% assign latestRelPost = (site.categories.releases | first) %}
{% assign latestRelVer = latestRelPost.title %}
{% assign latestSnapPost = (site.categories.snapshots | first) %}
{% assign latestSnapVer = latestSnapPost.title %}
# GET pgjdbc-ng

## MAVEN

### Releases
Available in Maven Central:

#### Driver

	<dependency>
		<groupId>com.impossibl.pgjdbc-ng</groupId>
		<artifactId>pgjdbc-ng</artifactId>
		<version>{{ latestRelVer }}</version>
	</dependency>

#### UDT Generator

	<dependency>
		<groupId>com.impossibl.pgjdbc-ng</groupId>
		<artifactId>pgjdbc-ng-udt</artifactId>
		<version>{{ latestRelVer }}</version>
	</dependency>


### Snapshots
Available in OSS repository:

[https://oss.sonatype.org/content/repositories/snapshots/](https://oss.sonatype.org/content/repositories/snapshots/)

#### Driver

	<dependency>
		<groupId>com.impossibl.pgjdbc-ng</groupId>
		<artifactId>pgjdbc-ng</artifactId>
		<version>{{ latestSnapVer }}-SNAPSHOT</version>
	</dependency>


#### UDT Generator

	<dependency>
		<groupId>com.impossibl.pgjdbc-ng</groupId>
		<artifactId>pgjdbc-ng-udt</artifactId>
		<version>{{ latestSnapVer }}</version>
	</dependency>


## DOWNLOAD
Download driver JAR:

## Driver

### Releases
{% for post in site.categories.releases %}
* [{{post.title}} JAR](releases/pgjdbc-ng-{{ post.title }}-complete.jar)
{% endfor %}

### Snapshots
{% for post in site.categories.snapshots %}
* [{{post.title}} JAR](snapshots/pgjdbc-ng-{{ post.title }}-SNAPSHOT-complete.jar)
{% endfor %}


## UDT Generator

### Releases
{% for post in site.categories.releases %}
* [{{post.title}} JAR](releases/pgjdbc-ng-udt-{{ post.title }}-complete.jar)
{% endfor %}

### Snapshots
{% for post in site.categories.snapshots %}
* [{{post.title}} JAR](snapshots/pgjdbc-ng-udt-{{ post.title }}-SNAPSHOT-complete.jar)
{% endfor %}
