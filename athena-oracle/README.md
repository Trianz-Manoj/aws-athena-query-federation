# Amazon Athena Lambda Oracle Connector

This connector enables Amazon Athena to access your Oracle databases.

**Athena Federated Queries are now enabled as GA in us-east-1, us-east-2, us-west-2, eu-west-1, ap-northeast-1, ap-south-1, us-west-1, ap-southeast-1, ap-southeast-2, eu-west-2, ap-northeast-2, eu-west-3, ca-central-1, sa-east-1, and eu-central-1. To use this feature, upgrade your engine version to Athena V2 in your workgroup settings. Check documentation here for more details: https://docs.aws.amazon.com/athena/latest/ug/engine-versions.html.**

By using this connector, you acknowledge the inclusion of third party components, a list of which can be found in [`pom.xml`](https://github.com/awslabs/aws-athena-query-federation/blob/master/athena-oracle/pom.xml) and agree to the terms in their respective licenses, provided in [`LICENSE.txt`](https://github.com/awslabs/aws-athena-query-federation/blob/master/athena-oracle/LICENSE.txt).

# Terms

* **Database Instance:** Any instance of a database deployed on premises, EC2 or using RDS.
* **Handler:** A Lambda handler accessing your database instance(s). Could be metadata or a record handler.
* **Metadata Handler:** A Lambda handler that retrieves metadata from your database instance(s).
* **Record Handler:** A Lambda handler that retrieves data records from your database instance(s).
* **Composite Handler:** A Lambda handler that retrieves metadata and data records from your database instance(s). This is recommended to be set as lambda function handler.
* **Multiplexing Handler:** a Lambda handler that can accept and use multiple different database connections.
* **Property/Parameter:** A database property used by handlers to extract database information for connection. These are set as Lambda environment variables.
* **Connection String:** Used to establish connection to a database instance.
* **Catalog:** Athena Catalog. This is not a Glue Catalog. Must be used to prefix `connection_string` property.

# Usage

## Parameters

The Oracle Connector supports several configuration parameters using Lambda environment variables.

### Connection String:

A JDBC Connection string is used to connect to a database instance. Following format is supported: `oracle://${jdbc_connection_string}`.

### Multiplexing handler parameters

Multiplexer provides a way to connect to multiple database instances using a single Lambda function. Requests are routed depending on catalog name. Use following classes in Lambda for using multiplexer.

|Handler|Class|
|---	|---	|
|Composite Handler|OracleMuxCompositeHandler|
|Metadata Handler|OracleMuxMetadataHandler|
|Record Handler|OracleMuxRecordHandler|


**Parameters:**

```
${catalog}_connection_string    Database instance connection string. One of two types specified above. Required.
                                Example: If the catalog as registered with Athena is myoraclecatalog then the environment variable name should be myoraclecatalog_connection_string

default                         Default connection string. Required. This will be used when catalog is `lambda:${AWS_LAMBDA_FUNCTION_NAME}`.
```

Example properties for a Oracle Mux Lambda function that supports two database instances, oracle1(default) and oracle2:

|Property|Value|
|---|---|
|default|oracle://jdbc:oracle:thin:${Test/RDS/Oracle1}@//oracle1.hostname:port/servicename|
|	|	|
|oracle_catalog1_connection_string|oracle://jdbc:oracle:thin:${Test/RDS/Oracle2}@//oracle1.hostname:port/servicename|
|	|	|
|oracle_catalog2_connection_string|oracle://jdbc:oracle:thin:${Test/RDS/Oracle2}@//oracle2.hostname:port/servicename|

Oracle Connector supports substitution of any string enclosed like *${Test/RDS/Oracle}* with *username* and *password* retrieved from AWS Secrets Manager. Example:

```
oracle://jdbc:oracle:thin:${Test/RDS/Oracle}@//hostname:port/servicename
```

will be modified to:

```
oracle://jdbc:oracle:thin:username/password@//hostname:port/servicename
```

Secret Name `Test/RDS/Oracle` will be used to retrieve secrets.

Currently Oracle recognizes without key `user` and `password` JDBC properties. It will take username and password like username/password without any key `user` and `password`

### Single connection handler parameters

Single connection metadata and record handlers can also be used to connect to a single Oracle instance.
```
Composite Handler    OracleCompositeHandler
Metadata Handler     OracleMetadataHandler
Record Handler       OracleRecordHandler
```

**Parameters:**

```
default         Default connection string. Required. This will be used when a catalog is not recognized.
```

These handlers support one database instance and must provide `default` connection string parameter. All other connection strings are ignored.

**Example property for a single oracle instance supported by a Lambda function:**

|Property|Value|
|---|---|
|default|oracle://jdbc:oracle:thin:${Test/RDS/Oracle}@//hostname:port/servicename|

### Spill parameters:

Lambda SDK may spill data to S3. All database instances accessed using a single Lambda spill to the same location.

```
spill_bucket    Spill bucket name. Required.
spill_prefix    Spill bucket key prefix. Required.
spill_put_request_headers    JSON encoded map of request headers and values for the s3 putObject request used for spilling. Example: `{"x-amz-server-side-encryption" : "AES256"}`. For more possible headers see: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html
```

# Data types support

|Jdbc|*Oracle[]|Arrow|
| ---|---|---|
|Boolean|boolean[]|Bit
|Integer|**N/A**|Tiny
|Short|smallint[]|Smallint
|Integer|integer[]|Int
|Long|bigint[]|Bigint
|float|float4[]|Float4
|Double|float8[]|Float8
|Date|date[]|DateDay
|Timestamp|timestamp[]|DateMilli
|String|text[]|Varchar
|Bytes|bytea[]|Varbinary
|BigDecimal|numeric(p,s)[]|Decimal
|**\*ARRAY**|**N/A**|List|

See Oracle documentation for conversion between JDBC and database types.

# Secrets

We support two ways to input database username and password:

1. **AWS Secrets Manager:** The name of the secret in AWS Secrets Manager can be embedded in JDBC connection string, which is used to replace with `username` and `password` values from Secret. Support is tightly integrated for AWS RDS database instances. When using AWS RDS, we highly recommend using AWS Secrets Manager, including credential rotation. If your database is not using AWS RDS, store credentials as JSON in the following format `{“username”: “${username}”, “password”: “${password}”}.`. To use the Athena Federated Query feature with AWS Secrets Manager, the VPC connected to your Lambda function should have [internet access](https://aws.amazon.com/premiumsupport/knowledge-center/internet-access-lambda-function/) or a [VPC endpoint](https://docs.aws.amazon.com/secretsmanager/latest/userguide/vpc-endpoint-overview.html#vpc-endpoint-create) to connect to Secrets Manager.
2. **Connection String:** Username and password can be specified as properties in the JDBC connection string.

# Partitions and Splits
A partition is represented by a single partition column of type varchar. We leverage partitions defined on a Oracle table, and this column contains partition names. For a table that does not have partition names, * is returned which is equivalent to a single partition. A partition is equivalent to a split.

|Name|Type|Description
|---|---|---|
|PARTITION_NAME|Varchar|Named partition in MySql. E.g. P_2006_DEC,P_2006_AUG ..|

# Running Integration Tests

The integration tests in this module are designed to run without the prior need for deploying the connector. Nevertheless, the integration tests will not run straight out-of-the-box. Certain build-dependencies are required for them to execute correctly.
For build commands and step-by-step instructions on building and running the integration tests see the [Running Integration Tests](https://github.com/awslabs/aws-athena-query-federation/blob/master/athena-federation-integ-test/README.md#running-integration-tests) README section in the **athena-federation-integ-test** module.

In addition to the build-dependencies, certain test configuration attributes must also be provided in the connector's [test-config.json](./etc/test-config.json) JSON file.
For additional information about the test configuration file, see the [Test Configuration](https://github.com/awslabs/aws-athena-query-federation/blob/master/athena-federation-integ-test/README.md#test-configuration) README section in the **athena-federation-integ-test** module.

Once all prerequisites have been satisfied, the integration tests can be executed by specifying the following command: `mvn failsafe:integration-test` from the connector's root directory.
** Oracle integration Test suite will not create any Oracle RDS instances and Database Schemas, Instead it will use existing Oracle RDS or EC2 instances.

# Deploying The Connector

To use this connector in your queries, navigate to AWS Serverless Application Repository and deploy a pre-built version of this connector. Alternatively, you can build and deploy this connector from source follow the below steps or use the more detailed tutorial in the athena-example module:

1. From the **athena-federation-sdk** dir, run `mvn clean install` if you haven't already.
2. From the **athena-federation-integ-test** dir, run `mvn clean install` if you haven't already
   (**Note: failure to follow this step will result in compilation errors**).
3. From the **athena-jdbc** dir, run `mvn clean install`.
4. From the **athena-oracle** dir, run `mvn clean install`.
5. From the **athena-oracle** dir, run  `../tools/publish.sh S3_BUCKET_NAME athena-oracle` to publish the connector to your private AWS Serverless Application Repository. The S3_BUCKET in the command is where a copy of the connector's code will be stored for Serverless Application Repository to retrieve it. This will allow users with permission to do so, the ability to deploy instances of the connector via 1-Click form. Then navigate to [Serverless Application Repository](https://aws.amazon.com/serverless/serverlessrepo)

# JDBC Driver Versions

For latest version information see [pom.xml](./pom.xml).

# Limitations
* Write DDL operations are not supported.
* In Mux setup, spill bucket and prefix is shared across all database instances.
* Any relevant Lambda Limits. See Lambda documentation.

# Performance tuning

Oracle supports native partitions. Athena's lambda connector can retrieve data from these partitions in parallel. We highly recommend native partitioning for retrieving huge datasets with uniform partition distribution.
