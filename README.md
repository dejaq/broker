# DejaQ v.Alpha

DejaQ is a distributed messaging queue built for high-throughput persistent messages that have an arbitrary or time-based order. It allows a point-to-point scheduled message-oriented async communication for loosely coupled systems in time and space (reference). Its main use case is to allow consumers to process messages that are produced out of order (the messages are consumed in a different order than they were produced).

Introduction and topics
DejaQ started as a learning project, but we soon realized that to tackle this problem we need more than one weekend. Taking on the challenge, we pursued and tackled each issue we encountered until we found a simple but scalable solution.

Official page: https://dejaq.io/

# Alpha version

We have high hopes for this project but in the first iteration, this alpha version we will only deliver an MVP that will contain:

* `Priority Queue` with uint16 priorities (only one type of topic)
* `High availability` You can run the broker in 3 nodes (or more 5,7...to be fault-tolerant to 1, 2 ... nodes)
* `Durability` All nodes will have the entire dataset saved on disk (using Raft for consensus and BadgerDB for embedded storage)
* `Consistency`: The writes will be acknowledged by the majority of the nodes and reads only served by the nodes that are up to date. Using Raft and BadgerDB we can provide a snapshot consistency. 
* `One Binary`: All you need to run is a single binary that will contain all the dependencies and logic
* `Easy to use API`: the first API will be HTTP based Swagger/OpenAPI compatible
* _theoretically_ unlimited topics with unlimited partitions, producers, and consumers
* `Dynamic consuming` One of the largest advantages of DejaQ is that topics are split into partitions (internally) and they are assigned dynamically at runtime. Although the algorithm in this alpha version is basic it will be extended in the future to allow more complex consuming strategies.
* `At least once delivery`, the best effort for exactly-once-delivery with the comment that the consumer has to successfully ack the processed messages in a timely manner (as long as it has its partitions assigned)
* `Ordering` is guaranteed at the consumer level (it will receive the smallest priority messages each pull from its assigned partitions). Different consumers may receive higher priority messages at the same time (because they consume from different partitions)

What we will NOT deliver in this alpha: 
* ACL, login/sessions
* Brokers auto-discovery 
* Leader brokers (that have the cluster metadata OR external ETCD cluster)
* Encryption: on-transit and on-rest
* Timeline topic (schedule message in the future where priority is the Unix TS + ability to cancel them)
* Horizontal scale (have partitions spread on nodes)
* Mutable messages, custom message IDs, cancel messages
* Producers can specify the partition (to allow consumers an ordered based processing)
* Cooperative dynamic consuming: Consumers are assigned priorities per partitions (multiple consumers can consume same lagging partitions)
* backup/restore capabilities
* PUSH based GRPC consumers


# Contributing 

See https://github.com/dejaq/broker/wiki/Contributing and choose your poison: https://github.com/dejaq/broker/issues

# Feedback

Can be provided here on Github or at team at dejaq.io
