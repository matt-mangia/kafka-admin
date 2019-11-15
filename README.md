## Table of Contents  
[Update Your Configuration File](#update-your-configuration-file)

[Build the Application Jar](#build-the-application-jar-file)

[Run the Jar](#run-the-jar-supplying-your-configuration-as-an-option)  

## Update your configuration file

### Add Cluster Configuration

In your `server.properties` file, add your cluster configuration as shown here:

```
bootstrap.servers: pkc-l7p2j.us-west-2.aws.confluent.cloud:9092
security.protocol: SASL_SSL
sasl.jaas.config: org.apache.kafka.common.security.plain.PlainLoginModule required username="<API-KEY>" password="<API-SECRET>";
sasl.mechanism: PLAIN
ssl.endpoint.identification.algorithm: https
```

Any cluster connection configuration properties may be specified here, just make sure they align to the actual Kafka client properties exactly. 
For example, you can add "ssl.enabled.mechanisms". 
Any properties that are not specified or are left with a blank value will use the default values. 

### Add/Update Topics

In your `config.yml` file, update the section under "topics" with your desired topics as shown here:

```
topics:
  matt-topic-1:
    name: matt-topic-1
    replication.factor: 1
    partitions: 3
    cleanup.policy: 
    compression.type: "lz4"
    retention.ms: "0"
```

You can choose to leave the additional configurations for a topic blank (and defaults will be used), or you can enter them as well under the desired topic. 
It is important to note that the **additional configuration values must be entered as strings (wrapped in double quotes)** or they will cause an error to the application. 

If you are you using delete topics (default is disabled), you will need to make sure all topics that exist on the cluster are in your "topics" section or exist in the "default_topics" section, as shown here:

```
default_topics:
  - _confluent-metrics
  - __confluent.support.metrics
```

If you're not using delete topics, then you do not need to worry about using the "default_topics" section. 

#### Increasing Topic Partitions

For a topic that already exists on the cluster, you can increase the number of partitions that the topic has by updating the configuration for the topic under the "topics" section.
You can only increase partitions -- **there is no ability to remove or reduce partitions.** 

Note that at this time, no other topic configurations can be modified by updating the config file. 

### Add/Update/Remove ACLs

In your `config.yml` file, update the section under "acls" with your complete list of acls as shown in the examples below:

#### ACLs to Produce to topic(s)

To produce to a topic, the principal of the producer will require the **WRITE** operation on the **topic** resource.

Example 1 -- ACLs needed for Service Account user "12576" to produce to a Topic named "griz-test" from any host:

```
acls:
    .
    .
    .
  project-xyz:
    resource-type: TOPIC
    resource-name: griz-test
    resource-pattern: LITERAL
    principal: User:12576
    host: '*'
    operation: WRITE
    permission: ALLOW
    .
    .
    . 
```

Example 2 -- ACLs needed for user "12576" to produce to **any topic whose name starts with "griz-"** from any host:

```
acls:
    .
    .
    .
  project-xyz:
    resource-type: TOPIC
    resource-name: griz-
    resource-pattern: PREFIXED
    principal: User:12576
    host: '*'
    operation: WRITE
    permission: ALLOW
    .
    .
    . 
```

_Producers may be configured with enable.idempotence=true to ensure that exactly one copy of each message is written to the stream. 
The principal used by idempotent producers must be authorized to perform IdempotentWrite on the cluster._

Example 3 -- ACLs needed for user "12576" to produce idempotently to a Topic named "griz-test" from any host:

```
acls:
    .
    .
    .
  project-xyz-1:
    resource-type: TOPIC
    resource-name: griz-test
    resource-pattern: LITERAL
    principal: User:12576
    host: '*'
    operation: WRITE
    permission: ALLOW
  project-xyz-2:
    resource-type: TOPIC
    resource-name: griz-test
    resource-pattern: LITERAL
    principal: User:12576
    host: '*'
    operation: IDEMPOTENT-WRITE
    permission: ALLOW
    .
    .
    . 
```

_Producers may also be configured with a non-empty transactional.id to enable transactional delivery with reliability semantics that span multiple producer sessions. 
The principal used by transactional producers must additionally be authorized for **Describe** and **Write** operations on the configured transactional.id._

Example 4 -- ACLs needed for user "12576" to produce using a transactional producer with transactional.id=test-txn to a Topic named "griz-test" from any host:

```
acls:
    .
    .
    .
  project-xyz-1:
    resource-type: TOPIC
    resource-name: griz-test
    resource-pattern: LITERAL
    principal: User:12576
    host: '*'
    operation: WRITE
    permission: ALLOW
  project-xyz-2:
    resource-type: TRANSACTIONALID
    resource-name: test-txn
    resource-pattern: LITERAL
    principal: User:12576
    host: '*'
    operation: DESCRIBE
    permission: ALLOW
  project-xyz-3:
    resource-type: TRANSACTIONALID
    resource-name: test-txn
    resource-pattern: LITERAL
    principal: User:12576
    host: '*'
    operation: WRITE
    permission: ALLOW
    .
    .
    . 
```

#### ACLs to Consume from topic(s)

To consume from a topic, the principal of the consumer will require the **READ** operation on the **topic** and **group** resources.

Example 1 -- ACLs needed for Service Account user "12576" to read from a topic named "griz-test" as any consumer group from any host:

```
acls:
    .
    .
    .
  project-xyz-1:
    resource-type: TOPIC
    resource-name: griz-test
    resource-pattern: LITERAL
    principal: User:12576
    host: '*'
    operation: READ
    permission: ALLOW
  project-xyz-2:
      resource-type: GROUP
      resource-name: '*'
      resource-pattern: LITERAL
      principal: User:12576
      host: '*'
      operation: READ
      permission: ALLOW
    .
    .
    . 
```

Example 2 -- ACLs needed for user "12576" to consume from **any topic whose name starts with "griz-"** as any consumer group from any host:

```
acls:
    .
    .
    .
  project-xyz-1:
    resource-type: TOPIC
    resource-name: griz-
    resource-pattern: PREFIXED
    principal: User:12576
    host: '*'
    operation: WRITE
    permission: ALLOW
  project-xyz-2:
      resource-type: GROUP
      resource-name: '*'
      resource-pattern: LITERAL
      principal: User:12576
      host: '*'
      operation: READ
      permission: ALLOW
    .
    .
    . 
```

For a list of all operations & resources, see this [link](https://docs.confluent.io/current/kafka/authorization.html#acl-format "Confluent Docs").

**Note that all fields are required for ACLs (unlike topics) in your config.yml file. 
Any ACLs that are not included in your list but that exist on the Kafka cluster will be removed when the application is run.** 

#### Adding multiple acls with a single acl configuration block:

You can add multiple acls using the same configuration block under acls. 
These can be easily grouped together into a project heading. 

Example -- The following block will create acls that give 'READ' and 'WRITE' to Matt, Brien, Aleks, Vedanta, and Liying for topics 'matt-topic-A','brien-topic', and 'third-topic':

```
acls:
  project-1:
    resource-type: topic
    resource-name: matt-topic-A, brien-topic, third-topic
    resource-pattern: LITERAL
    principal: User:Matt, User:Brien, User:Aleks, User:Vedanta,User:Liying
    operation: READ, WRITE
    permission: ALLOW
    host: '*'
```

## Build the application jar file

From the project root directly, run the following:

`mvn clean package`

## Run the Jar, supplying your configuration with the "-properties" or "-p" and -config" or "-c" options to generate your topic & ACL plans

`java -jar <path-to-jar> -properties <path.properties> -config <path-config.yml>` 

## Run the Jar, supplying your configuration as above & adding the "-execute" option to actually execute the plans

`java -jar <path-to-jar>  -properties <path.properties> -config <path-config.yml> -execute`

## Run the Jar, supplying your connection configuration with the "-properties" or "-p" and "-dump" options to print out current topic/ACLs configuration to stdout

`java -jar <path-to-jar>  -properties <path.properties> -dump`

## Run the Jar, supplying your connection configuration with the "-properties" or "-p", "-dump" and "-output" or "-o" options to print out current topic/ACLs configuration to a file

`java -jar <path-to-jar>  -properties <path.properties> -dump -output <path-output.yml>`

