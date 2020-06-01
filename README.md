### GraphQL API for Kafka

A GraphQL interface to control a Kafka cluster.

## Current list of supported operations
* **topics**  
Returns names for all topics in the cluster
* **topic**(topic: String)  
Return metadata about a specific topic
* **partitionMetaData**(topic: String, partition: Int)  
Return metadata about a specific partition
* **getOffsets**(topic: String, partition: Int)  
Return offsets for a specific partition
* **addConsumer**(group: String, consumer: Consumer)  
Create a consumer
* **subscribe**(group: String, instance: String, topics: [String])  
Subscribe a consumer to a list of topics
* **getSubscriptions**(group: String, instance: String)  
Return subscriptions for a consumer instance
* **consumeBinary**(group: String, instance: String)  
Returns a consumed array of String records from a consumer
* **consumeAvro**(group: String, instance: String)  
Returns a consumed array of Avro records from a consumer
* **produceBinary**(topic: String, records: [Record])  
Produce an array of String records to a topic
* **produceAvro**(topic: String, records: [AvroRecord])  
Produce an array of Avro records to a topic
