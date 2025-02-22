document-SQL generator
======================

This library attempts to bridge the gap between the flexibility of document storage and the comfort of SQL schemata. Typically, XML and JSON documents follow a schema, this library creates DDL to represent these schemata as SQL views of a stored document. A schema is generated from its Java class representation and written to DDL.

As an example, consider the following class:

```java
class Sample {
  String val;
}
```

Using the document-SQL generator, a view `SAMPLE` with the columns `FOO` and `BAR` can be created to persist XML or JSON representations of the processed type, here using Oracle as an example:

```java
JdbcDispatcher<String> dispatcher = JdbcDispatcher
  .of(OracleDispatcherFactory.ofJson())
  .build("SAMPLE", Sample.class);

dispatcher.create(dataSource);
dispatcher.insert(dataSource, "myid", "{\"val\":\"value\"}");
```

As a result, a view `SAMPLE` is created within the specified datasource that allows for the following query:

```sql
SELECT ID FROM SAMPLE WHERE VAL = 'value'
-- 1 row: ID = 'myid'
```

Currently, this library offers an implementation for Oracle and Postgres for both XML and JSON. The generated views are fully indexed what allows for efficient searches even with big data sets.

Nested classes are resolved accordingly. If instead of Sample, the following wrapper was supplied:

```java
class Wrapper {
  String val;
  Sample nested;
}
```

the view's columns would be named accordingly as `VAL` and `NESTED_VAL`. Note that a Java class might result in multiple views since collections cannot be represented in the same view within the relational model:

```java
class Wrapper {
  String val;
  List<Sample> nested;
}
```

which would result in to views `WRAPPER` and `WRAPPER_SAMPLE`. All views define the `ID` column to uniquely identify a document what allows for joins between these views. To make navigation easier, a utility view 
`WRAPPER_MTA` is defined which maps all fully qualified JSON- or XPATH of a document to their tables and columns.

Besides `ID`, any stored document offers a `REVISION` and a `DELETED` property. The `REVISION` property allows to identify a document's latest version. The `DELETED` property allows to mark documents as deleted. To make simple use of these columns, three additional views are created:
- `SAMPLE_MIN`: a view containing `ID`, `REVISION` tuples of only the oldest document per id.
- `SAMPLE_MAX`: a view containing `ID`, `REVISION` tuples of only the newest document per id.
- `SAMPLE_NOW`: the former view but without documents with the latest revision being deleted.

The `JdbcDispatcher` offers overloads for adding such meta data and also allows for batch inserts and inserts within an ongoing transaction. It is also possible to only process a subelement of any document by providing a list of root elements, relative to which all view representations are built. XML documents should always include a single root element as XML requires it.

Schema customization
--------------------

When building a `JdbcDispatcher`, several customizations can be applied:
- `ViewResolver`: A view resolver is responsible to create a view model of a given Java class. By default, objects are processed until a known terminal type (Java primitives, their wrappers, known number or date types, `Object` or `String`) is discovered. The processing behavior can however be adjusted to for example consider JAXB or Jackson annotations, to consider different terminal types, or to ignore subpaths of a Java class. Instead of implementing a `ViewResolver` from scratch, the bundled `SimpleViewResolver` accepts a path resolver such as `JaxbPathResolver` or `JacksonJsonPathResolver`. It is also possible to supply a custom subpath filter or a predicate to identify terminal types.
- `TableResolver`: A table resolver allows to define additional meta data that is included in any view. It also allows to define a method to resolve a Java object to a string for which `JdbcDispatcher` is generified.
- `NameResolver`: A name resolver allows to determine the name of views and their columns. By default, names are capitalized to fit the SQL standard. The bundled `CapitalizingNameResolver` does however set a length limit and abbreviates names if required.

A `JdbcDispatcherFactory` allows for further customizations. The `OracleDispatcherFactory` for example provides the following options:
- A type resolver allows to determine how a view should represent Java types as SQL data types. For example, an `int` can be represented as a `NUMBER`, a `String` as a `VARCHAR`.
- The *audit* property enables select auditing on all views. To query audit information, this project also includes an `AuditView` API.
- The *meta* property determines if the previously described meta table should be created.
- The *synonym* property determines if public synonyms should be created for all views.
- The *grantViewOnDummy* property determines if privileges to views should be granted on pseudo views before the actual views are defined. Due to a bug in Oracle, granting access to complex views might crash the database. Users and roles to grant privileges to can be specified in the `JdbcDispatcher` upon creating the dispatcher.

Schema management
-----------------

Typically, applications should only create the document views a single time. The *document-sql-management* module is dedicated to simplify the management of such previous creation.

A `SchemaContext` describes how a `JdbcDispatcher` should be created where the actual creation is applied by a `SchemaManager` which considers the state of a previous dispatcher creation against a state management database.

Note that most databases do not allow for the execution of DDL within a transaction. As a result, a database can reach an inconsistent state. A `SchemaManager` makes a best effort to unroll previous, failed schema creations. Additionally, it is able to discover changes of a `SchemaContext` to fail a creation or to recreate a schema if specified.

The schema management artifact supplies a Liquibase configuration to create the backing database table.

Database monitors
-----------------

For implementation convenience, the API module includes monitoring interfaces that can optionally be implemented for each database:

- `AuditMonitor`: offers a summary for reading an access log.
- `SizeMonitor`: offers a summary for reading the (approximative) size being used per document group.

Implementation details
----------------------

The implementation is split into two base areas: 

1. In a first step, a Java class is resolved to a mapping of property paths to values that represent the Java class. Typically, these paths are XPaths or JSON-paths. A `ViewResolver` is responsible for this path resolution. The `SimpleViewResolver` that is bundled with this distribution implements a non-recursive resolution that is general enough to handle both XML and JSON path extraction while also detecting recursive properties. To construct it, it requires a path resolver, which can for example be a `JaxbPathResolver` to consider JAXB annotations on class properties, a predicate to determine terminal properties, i.e. properties that complete a path to a property, and an optional filter that determines when to abort path resolution. Finally, an optional root resolver can be used to determine if one or more root elements need to be prepended to any property path, for example for an XML document that requires a root what can be resolved using a `JaxbRootResolver`. Root elements can also be specified manually, for example if only a subtree of a document's properties is to be represented. 

2. In a second step, a `JdbcDispatcherFactory` is responsible for translating a mapping of property paths to an SQL view representation. With this distribution comes an implementation for Oracle with `OracleDispatcherFactory` where each path is represented as a property of an [*XMLTABLE*](https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/XMLTABLE.html#GUID-C4A32C58-33E5-4CF1-A1FE-039550D3ECFA) which are indexed via an [*XMLINDEX*](https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/CREATE-INDEX.html#GUID-1F89BBC0-825F-4215-AF71-7588E31D8BFE) for XML whereas JSON is represented as a [*JSON_TABLE*](https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/JSON_TABLE.html#GUID-3C8E63B5-0B94-4E86-A2D3-3D4831B67C62) within directly indexed materialized views. The base data is in contrast only stored in the original format within a *RAW* data table and can be queried via these views. Similarly, `PostgresDispatcherFactory` offers representation of XML via *XMLTABLE* and JSON via *JSONB_PATH_QUERY* (requires Postgres 12 or newer).

For supporting additional databases, it is therefore only required to implement a `JdbcDispatcherFactory` whereas the existing view resolution can be resused. Similarly, it is possible to customize view resolution without requiring a change in the SQL generation.
