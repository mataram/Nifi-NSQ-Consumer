# ConsumeNSQ Apache Nifi Processor
A custom processor for fetches messages from NSQ. When a message is received from NSQ, this Processor emits a FlowFile where the content of the FlowFile is the value of the NSQ message.

## Requirements
- Java 8
- Nifi 1.x
- Maven 2 or above

## Build and Generate nar file
Plase run command bellow to build the processor
```
mvn clean install
```
Once maven install is done you will have the nar file at the target directory with name **nifi-nsq-nar-0.1.nar** inside directory **nifi-nsq-nar/target**

## Install the processor to Nifi
Copy the nar file to the lib directory where nifi is installed