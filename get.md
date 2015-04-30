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

* [0.0.2]({{site.baseurl}}/releases/pgjdbc-ng-udt-0.0.2-complete.jar)

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
		<version>0.0.2</version>
		<classifier>complete</classifier>
	</dependency>

<!---
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

-->