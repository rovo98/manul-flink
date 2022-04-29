## Extended connector jdbc

author: rovo98

## Intro

Flink 应用在 RDB 数据摄入和写出场景中，我们一般会使用到 ``flink-connector-jdbc``，该连接器提供了 DataStream 的 ``JdbcSink.sink``
、``JdbcSink.exactlyOnceSink`` 来支持 jdbc 的数据写，而在 Table API 层面则提供了 SQL 查询方式的建表数据读写支持。使用该连接器可有效地减少我们开发具体应用时难度和时间。当然，我们也可以使用
Function API 来实现具体的 Source 或 Sink ，以适应项目需求，但这也存在一定局限性，首先是开发者需要对低级的 API 有一定的了解，且能够使用合适的 API 来实现，其次是基于 ``SourceFunction``
实现的 Source 有一定局限性，如只能以 Streaming 模式执行，且自定义实现通常是具体化的，我们更多的是希望能够功能通用化，以支撑更多项目及更多场景。

在开发实践中，使用 ``flink-connector-jdbc`` 也难免会遇到一些难题。在 Flink 1.15 之前的版本中，由于社区版 JDBC 连接器仅支持 Derby、MySQL、Postgresql
驱动，因此，如不对社区版本的 JDBC 连接器进行修改，则无法使用 Table/SQL API 来创建 Table Source/Sink。此外，通过 Table/SQL API 创建的 JDBC 源表，如不采用分区扫描，则无法充分发挥
TM 任务槽资源进行并行的数据加载，还可能导致 TM OOM/GC 问题的出现；另外，社区版本的 ``flink-connector-jdbc`` 使用 Table/SQL API
建表时，向数据库源表读取数据时会先进行不带条件的全表扫描查询，这种情况下，除非我们应用真的需要用到源表全部数据，不然则会导致不必要的数据计算和传输开销，当源数据表数据量大时，还可能导致 OOM 问题的产生。

因此，为了解决项目开发过程中使用社区版 ``flink-connector-jdbc`` 模块遇到的上述问题，我们针对该模块进行了二次开发，实现功能扩展。后文统一称扩展后的 JDBC
连接器模块为 ``flink-manul-connector-jdbc``。

``flink-manul-connector-jdbc`` 扩展功能点如下：

1. 采用 SPI， 以扩展支持更多 ``JdbcDialect``；
2. Table/SQL API Source 表定义支持字符串字段分区；
3. Table/SQL API Source 表定义支持给 query 添加查询条件，谓词下推至数据库源；
4. 使用流批一体的新版 Source API 实现 ``JdbcSource``；

## 功能说明

### 1. JdbcDialect 扩展

本项目开箱支持了 Derby、MySQL、PostgreSQL 以及 Oracle JdbcDialect，如要扩展支持更多驱动，可以选择继承 ``AbstractDialect`` 或实现
``JdbcDialect`` 接口。然后在相应的 SPI 服务配置文件进行注册。例如：

 ```java
public class OracleDialect extends AbstractDialect {
    private static final long serialVersionUID = 1L;

    // Define  MAX/MIN precision of TIMESTAMP type according to Oracle docs:
    // https://www.techonthenet.com/oracle/datatypes.php
    private static final int MAX_TIMESTAMP_PRECISION = 9;
    private static final int MIN_TIMESTAMP_PRECISION = 1;

    // Define MAX/MIN precision of decimal type according to Oracle docs:
    // https://www.techonthenet.com/oracle/datatypes.php
    private static final int MAX_DECIMAL_PRECISION = 38;
    private static final int MIN_DECIMAL_PRECISION = 1;

    @Override
    public String dialectName() {
        return "Oracle";
    }

    @Override
    public boolean canHandle(String url) {
        return url.startsWith("jdbc:oracle:");
    }

    @Override
    public JdbcRowConverter getRowConverter(RowType rowType) {
        return new OracleRowConverter(rowType);
    }

    @Override
    public String getLimitClause(long limit) {
        return "FETCH FIRST " + limit + " ROWS ONLY";
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("oracle.jdbc.OracleDriver");
    }

    @Override
    public String quoteIdentifier(String identifier) {
        // if we use double-quotes identifier then Oracle becomes case-sensitive
        return identifier;
    }

    @Override
    public Optional<String> getUpsertStatement(
            String tableName, String[] fieldNames, String[] uniqueKeyFields) {

        String sourceFields =
                Arrays.stream(fieldNames)
                        .map(f -> ":" + f + " " + quoteIdentifier(f))
                        .collect(Collectors.joining(", "));

        String onClause =
                Arrays.stream(uniqueKeyFields)
                        .map(f -> "t." + quoteIdentifier(f) + "=s." + quoteIdentifier(f))
                        .collect(Collectors.joining(" and "));

        final Set<String> uniqueKeyFieldsSet =
                Arrays.stream(uniqueKeyFields).collect(Collectors.toSet());
        String updateClause =
                Arrays.stream(fieldNames)
                        .filter(f -> !uniqueKeyFieldsSet.contains(f))
                        .map(f -> "t." + quoteIdentifier(f) + "=s." + quoteIdentifier(f))
                        .collect(Collectors.joining(", "));

        String insertFields =
                Arrays.stream(fieldNames)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));

        String valuesClause =
                Arrays.stream(fieldNames)
                        .map(f -> "s." + quoteIdentifier(f))
                        .collect(Collectors.joining(", "));

        // if we can't divide schema and the table-name is risky to call quoteIdentifier(tableName)
        // for example [tbo].[sometable] is ok but [tbo.sometable] is not
        String mergeQuery =
                " MERGE INTO "
                        + tableName
                        + " t "
                        + " USING (SELECT "
                        + sourceFields
                        + " FROM DUAL) s "
                        + " ON ("
                        + onClause
                        + ") "
                        + " WHEN MATCHED THEN UPDATE SET "
                        + updateClause
                        + " WHEN NOT MATCHED THEN INSERT ("
                        + insertFields
                        + ")"
                        + " VALUES ("
                        + valuesClause
                        + ")";

        return Optional.of(mergeQuery);
    }

    @Override
    public int maxDecimalPrecision() {
        return MAX_DECIMAL_PRECISION;
    }

    @Override
    public int minDecimalPrecision() {
        return MIN_DECIMAL_PRECISION;
    }

    @Override
    public int maxTimestampPrecision() {
        return MAX_TIMESTAMP_PRECISION;
    }

    @Override
    public int minTimestampPrecision() {
        return MIN_TIMESTAMP_PRECISION;
    }

    @Override
    public List<LogicalTypeRoot> unsupportedTypes() {
        // The data type used in Oracle are list at:
        // https://www.techonthenet.com/oracle/datatypes.php

        return Arrays.asList(
                LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
                LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE,
                LogicalTypeRoot.INTERVAL_YEAR_MONTH,
                LogicalTypeRoot.INTERVAL_DAY_TIME,
                LogicalTypeRoot.MULTISET,
                LogicalTypeRoot.NULL,
                LogicalTypeRoot.MAP,
                LogicalTypeRoot.ROW,
                LogicalTypeRoot.DISTINCT_TYPE,
                LogicalTypeRoot.STRUCTURED_TYPE,
                LogicalTypeRoot.RAW,
                LogicalTypeRoot.SYMBOL,
                LogicalTypeRoot.UNRESOLVED);
    }
}
```

SPI 服务注册：

``resource/META-INF/services/org.apache.flink.connector.jdbc.dialect.JdbcDialect``

```txt
com.rovo98.flink.manul.connector.jdbc.dialect.OracleDialect
```

### 2. Table/SQL Source 表支持字符串类型分区

Flink 社区版 connector-jdbc 仅支持 numeric/time/datetime 的范围分区
[partitioned scan](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/connectors/table/jdbc/#partitioned-scan)
，在指定 ``scan.partition.column`` 的同时，还需提供 ``scan.partition.num`` 以及 ``scan.partition.lower-bound``
和 ``scan.partition.upper-bound``。

在某些场景中，我们希望可以根据离散的字符串字段来进行分区加载数据。**当然，需要注意的是，作为分区字段的字符串取值不应该过多，合适的类型可以是 地区编码、系统代码等**。

为此，我们基于 ``scan.partition.column`` 增加了两个配置项 - ``scan.partition.string-type`` 和 ``scan.partition.string-column-values`` 。

| Option | Default | Description |
| :-----:|:-------:|:-----------:|
| scan.partition.string-type| false| 和 scan.partition.column 配置配合使用，true 表示分区字段为字符串类型|
| scan.partition.string-column-values| 无 | 逗号隔开的字符串字段值，仅在 scan.partition.string-type=true 时起作用|

使用示例：

```java
String ddlQuery=String.format(
        "create table %s (\n"
        +"%s\n"
        +") with (\n"
        +"'connector' = 'manul-jdbc',\n"
        +"'table-name' = '%s',\n"
        +"'url' = '%s',\n"
        +"'driver' = '%s',\n"
        +"'username' = '%s',\n"
        +"'password' = '%s',\n"
        +"'scan.partition.column' = 'area_code',\n"
        +"'scan.partition.string-type' = 'true',\n"
        +"'scan.partition.string-column-values' = '%s'\n",
        registerTblName,
        fieldsDef,
        sourceTblName,
        jdbcConf.getProperty("jdbcUrl"),
        jdbcConf.getProperty("driver"),
        jdbcConf.getProperty("username"),
        jdbcConf.getProperty("password"),
        "01,02,03,05,07"
        );

tEnv.executeSql(ddlQuery);
```

### 3. Table/SQL API Source 表支持谓词下推至数据库源

在社区版 Flink 中， 使用 Table API connector 创建的 Jdbc Source，其执行的查询，除采用 partitioned scan
会给相应的查询按分区添加 ``where <field> between ? and ? `` 外，其他均是全表扫描，因此，如遇到源数据表数据量非常大的情况，
尽管 Table/SQL API 实现的程序会将谓词下推至最靠近数据源位置的算子 e.g. ``TableSourceScan``，但不可避免地，我们还是需要从数据库中加载大量不必要的数据到 Flink 中，
还可能引起 OOM 问题。例如：假设某一大表按年份存放了近 3 年的数据，每年份包含数据量大约 5 亿，那么该表总共约 15 亿数据量。如果应用业务计算只需要
某一年份的数据（e.g. ``... where year = 2022``），此时，数据库仍需要将全表数据传输给 Flink，然后 Flink 的 ``TableSourceScan`` 再进行筛选。
这一过程存在很多不必要的开销。

为解决上述问题，本项目给 Table API connector-jdbc 增加了一个 ``scan.query.push-down-constraints`` 选项，可将查询的 Where 子句的条件
下推至数据库源，使数据库执行查询带上条件，以减少查询计算、数据传输的开销。

使用示例：

```java
String ddlQuery=String.format(
        "create table %s (\n"
        +"%s\n"
        +") with (\n"
        +"'connector' = 'manul-jdbc',\n"
        +"'table-name' = '%s',\n"
        +"'url' = '%s',\n"
        +"'driver' = '%s',\n"
        +"'username' = '%s',\n"
        +"'password' = '%s',\n"
        +"'scan.query.push-down-constraints' = 'year = 2022 and area_code in (03, 05)'\n",
        registerTblName,
        fieldsDef,
        sourceTblName,
        jdbcConf.getProperty("jdbcUrl"),
        jdbcConf.getProperty("driver"),
        jdbcConf.getProperty("username"),
        jdbcConf.getProperty("password")
        );
tEnv.executeSql(ddlQuery);
```

| Option | Default |             Description              |
|:------:|:-------:|:------------------------------------:|
| scan.query.push-down-constraints| 无 | Where 子句查询条件，即 boolean 表达式, 能下推至数据库源 |

### 4. 基于 new Source API 实现的 JdbcSource

Flink 新版 [Source API](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/dev/datastream/sources/)
由 [FLIP-27](https://cwiki.apache.org/confluence/display/FLINK/FLIP-27%3A+Refactor+Source+Interface)所引入，旨在实现流批一体的 Source
接口，目前社区不少连接器适配了新版 Source
API，如 [KafkaSource](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/connectors/datastream/kafka/)
、[FileSource](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/connectors/datastream/filesystem/) 等。

![](../images/flink-new-source-api.png)

上图是 Source API 涉及的主要组件的交互示意图，具体文档建议查看 Flink 官网以及 FLIP-27的内容。

近期（2022年6月），Flink 社区也在讨论将原来 ``SourceFunction``  标记为过时，并谈论开展一系列新版 Source API 迁移任务。

- https://www.mail-archive.com/dev@flink.apache.org/msg57564.html
- https://issues.apache.org/jira/browse/FLINK-28045

本文介绍的 JDBC 扩展模块则是因为项目开发需求，先于社区且在较低 Flink 版本（1.12 - 1.14）版本中实现了 ``JdbcSource`` 接口（当然，考虑的场景可能没那么多，相对于社区，想法和设计实现可能比较
trivial），使得 Flink 应用开发更容易集成 JDBC RDB 数据，并可选择以流或批模式进行作业任务的执行（流批一体）。

#### JdbcSource API

JdbcSource 是一个创建上述 Source API 各组件的入口类，为了方便用户使用，通常会提供一个工厂方法，以创建出具体的实例。如 ``JdbcSource#source``:

```java
    /**
 * Factory method to create a JdbcSource.
 *
 * @param query A string query which can contains parameter placeholder {@code ?}
 * @param parameterValuesProvider This provides the values for the parameters in the query.
 * @param resultSetValueExtractor To extract object of target type from the given {@link java.sql.ResultSet}.
 * @param connectionOptions options to build jdbc connection
 * @param bounded specify whether this source is bounded or unbounded.
 * @return Created {@link JdbcSource} instance
 * @param <T> the type of the query result.
 */
public static <T> JdbcSource<T> source(
        String query,
        JdbcParameterValuesProvider parameterValuesProvider,
        ResultSetValueExtractor<T> resultSetValueExtractor,
        ManulJdbcConnectionOptions connectionOptions,
        boolean bounded) {
        // validate connectionOptions
        validateOptions(connectionOptions);
        return new JdbcSource<>(
        query,
        parameterValuesProvider,
        resultSetValueExtractor,
        connectionOptions,
        bounded ? Boundedness.BOUNDED : Boundedness.CONTINUOUS_UNBOUNDED);
}

public static <T> JdbcSource<T> source(
        String query,
        JdbcParameterValuesProvider parameterValuesProvider,
        ResultSetValueExtractor<T> resultSetValueExtractor,
        ManulJdbcConnectionOptions connectionOptions) {
        return source(
        query, parameterValuesProvider, resultSetValueExtractor, connectionOptions, true);
}
```

参数说明：

- ``query``: 要执行的 SQL 查询，可以带参数，用法与 ``PreparedStatement`` 一致；
- ``parameterValuesProvider``： 在 SQL 查询带参数的情况下使用，用于提供查询参数；无参数则提供 ``null`` 即可；
- ``resultSetValueExtractor``: 是一个函数式接口，用于从 ``ResultSet`` 中提取出具体的记录对象；
- ``connectionOptions``: 用户构建 JDBC 连接的参数配置；
- ``bounded``:  ``true`` 表示该 JdbcSource 以批模式运行，``false`` 则表示以流模式运行；

使用示例：

```java
...
// 查询参数组
Serializable[][] queryParameters = new String[2][1];
queryParameters[0] = new String[] {"001"};
queryParameters[1] = new String[] {"011"};

// 定义  JdbcSource
JdbcSource<String> source =
    JdbcSource.source(
        "select distinct area_code from dim_sys_org_info where province_code = ?",
        new JdbcGenericParameterValuesProvider(queryParameters),
        (resultSet -> resultSet.getString(0)),
        ManulJdbcConnectionOptions.builder()
        .withDriverName("com.seaboxsql.Driver")
        .withUrl("jdbc:seaboxsql://172.19.20.211:7300/testdb")
        .withUsername("rovo98")
        .withPassword("pwd@rovo98")
        .build());

// 使用
DataStreamSource<String> s =
    env.fromSource(
        source,
        WatermarkStrategy.forMonotonousTimestamps(),
        "fetchAreaCodeByGivenProvinceCode",
        TypeInformation.of(String.class));
s.print();
// other transformations
...
```


