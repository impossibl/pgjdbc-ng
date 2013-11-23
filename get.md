---
layout: default
---
{% assign latestRelPost = (site.categories.releases | first) %}
{% assign latestRelVer = latestRelPost.title %}
{% assign latestSnapPost = (site.categories.snapshots | first) %}
{% assign latestSnapVer = latestSnapPost.title %}
# GET pgjdbc-ng

## DOWNLOAD

## Driver

{% for post in site.categories.releases %}
* [{{post.title}} JAR](releases/pgjdbc-ng-{{ post.title }}-complete.jar)
{% endfor %}


## UDT Generator

{% for post in site.categories.releases %}
* [{{post.title}} JAR](releases/pgjdbc-ng-udt-{{ post.title }}-complete.jar)
{% endfor %}


## MAVEN

### Releases

Available in Maven Central:

#### Driver

	<dependency>
		<groupId>com.impossibl.pgjdbc-ng</groupId>
		<artifactId>pgjdbc-ng</artifactId>
		<version>{{ latestRelVer }}</version>
		<classifier>complete</classifier>
	</dependency>

#### UDT Generator

	<dependency>
		<groupId>com.impossibl.pgjdbc-ng</groupId>
		<artifactId>pgjdbc-ng-udt</artifactId>
		<version>{{ latestRelVer }}</version>
		<classifier>complete</classifier>
	</dependency>


### Snapshots
Available in OSS repository:

[https://oss.sonatype.org/content/repositories/snapshots/](https://oss.sonatype.org/content/repositories/snapshots/)

#### Driver

	<dependency>
		<groupId>com.impossibl.pgjdbc-ng</groupId>
		<artifactId>pgjdbc-ng</artifactId>
		<version>{{ latestSnapVer }}-SNAPSHOT</version>
		<classifier>complete</classifier>
	</dependency>


#### UDT Generator

	<dependency>
		<groupId>com.impossibl.pgjdbc-ng</groupId>
		<artifactId>pgjdbc-ng-udt</artifactId>
		<version>{{ latestSnapVer }}-SNAPSHOT</version>
		<classifier>complete</classifier>
	</dependency>

