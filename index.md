---
layout: default
---
## Features Included

* JDBC 4.1 target (with goal of complete conformance)
* UDT support through standard SQLData, SQLInput & SQLOutput
* Code Generator for UDTs ([(https://github.com/impossibl/pgjdbc-ng-udt)](https://github.com/impossibl/pgjdbc-ng-udt))
* Support for JDBC custom type mappings
* Pluggable custom type serialization
* Compact binary format with text format fallback
* Database, ResultSet and Parameter meta data
* Transactions / Savepoints
* Blobs
* Updatable ResultSets
* Callable Statements
* SSL Authentication and Encrpytion

## Features Coming Soon

* DataSources

## Releases

{% for post in site.categories.releases %}
* [ {{ post.title }} ]( {{ site.baseurl }}{{ post.url }} )  ( {{ post.date | date: "%b %d, %Y" }} )
{% endfor %}

## Snapshots

{% for post in site.categories.snapshots %}
* [ {{ post.title }}-SNAPSHOT ]( {{ site.baseurl }}{{ post.url }} ) ( {{ post.date | date: "%b %d, %Y" }} )
{% endfor %}

## Requirements

* Java 7
* PostgreSQL 9.2

## Sponsors

YourKit is kindly supporting open source projects with its full-featured Java Profiler.
YourKit, LLC is the creator of innovative and intelligent tools for profiling
Java and .NET applications. Take a look at YourKit's leading software products:
[YourKit Java Profiler](http://www.yourkit.com/java/profiler/index.jsp) and [YourKit .NET Profiler](http://www.yourkit.com/.net/profiler/index.jsp)
