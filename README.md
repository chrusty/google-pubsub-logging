# Google PUB/SUB logging
This project includes sample configurations for FluentD which will stream logs from your boxes onto Google's PUB/SUB, as well as a set of dataflows which can be used to process the messages once they're on PUB/SUB.

## FluentD config:
I've included sample configs for fluentd
* It will tail 4 important syslog files (syslog, auth.log, kern.log, fail2ban.log)
* It makes sure they have timestamp keys
* It then uses the "time_parser" plugin to add proper timestamp field to messages ("parsed_time") which BigQuery can parse into a column of the correct type ("TIMESTMAP")
* It then uses the "record-modifier" plugin to enrich messages (adding a "host" tag, an "environment" tag, and a "role" tag)
* It then uses the "gcloud_pubsub" plugin to publish messages to PUB/SUB (where the data-flows take over)

## DataFlowLoggingPubSubToCloudStorage
On the face of it this seems like the simplest data-flow, except for the following dilemma:
* In order to read from PUB/SUB a dataflow needs to run in streaming mode
* In order to write to CloudStorage a dataflow can't be running in streaming mode

I plan to come back to this later.

## DataFlowLoggingPubSubToBigQuery
This data-flow subscribes to a PUB/SUB topic, converts each message into a BigQuery row, and streams them into a BigQuery table. It is assumed that the messages are JSON objects with the following attributes:
* time: The time the message was generated
* host: The host that generated this message
* ident: A description of what service or process generated the message
* message: The message itself
* environment: The environment the message came from (eg "live" or "test")
* role: The role of the machine that generated the message (eg "webserver" or "mysql-database")

### Preparation:
* Create a PUB/SUB topic
* Create a cloud-storage bucket for the "staging" data (Java cruft uploaded by your SDK)
* Give ownership on the "STAGING" bucket to the app-engine account

### Options:
--project=logging
--stagingLocation=gs://logging/logtobq/staging
--runner=BlockingDataflowPipelineRunner
--bigQueryDataset=logs
--bigQueryTablePrefix=daily
--splitTables=true
--pubsubTopic=projects/logging/topics/logs
--jobName=logtobqdaily
--tempLocation=gs://logging-us/logtobq/temp
--streaming=true
--numWorkers=1
--maxNumWorkers=2

### ToDo:
* Give the PUB/SUB subscription a more descriptive name
