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

	<dependency>
		<groupId>com.impossibl.pgjdbc-ng</groupId>
		<artifactId>pgjdbc-ng</artifactId>
		<version>{{ latestRelVer }}</version>
	</dependency>

### Snapshots
Available in OSS repository:

[https://oss.sonatype.org/content/repositories/snapshots/](https://oss.sonatype.org/content/repositories/snapshots/)

	<dependency>
		<groupId>com.impossibl.pgjdbc-ng</groupId>
		<artifactId>pgjdbc-ng</artifactId>
		<version>{{ latestSnapVer }}-SNAPSHOT</version>
	</dependency>


## DOWNLOAD
Download driver JAR:

## Releases
{% for post in site.categories.releases %}
* [{{post.title}} JAR](releases/pgjdbc-ng-{{ post.title }}-complete.jar)
{% endfor %}

## Snapshots
{% for post in site.categories.snapshots %}
* [{{post.title}} JAR](snapshots/pgjdbc-ng-{{ post.title }}-SNAPSHOT-complete.jar)
{% endfor %}