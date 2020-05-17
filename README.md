# DejaQ v.Alpha

DejaQ is a distributed messaging queue built for high-throughput persistent messages that have an arbitrary or time-based order. It allows a point-to-point scheduled message-oriented async communication for loosely coupled systems in time and space (reference). Its main use case is to allow consumers to process messages that are produced out of order (the messages are consumed in a different order than they were produced).

Introduction and topics
DejaQ started as a learning project, but we soon realized that to tackle this problem we need more than one weekend. Taking on the challenge, we pursued and tackled each issue we encountered until we found a simple but scalable solution.

Official page: https://dejaq.io/

# Alpha1 version (Work In Progress)

We have high hopes for this project but in the first iteration, this alpha version we will only deliver an MVP that will contain:

* `Priority Queue` with messages that have priorities ranging between 0-65535
* `High availability / distributed` You can run the brokers as a 3nodes (or more) cluster to increase the fault-tolerance and durability.
* `Durability` All nodes will have the entire dataset saved on disk (using Raft for consensus and BadgerDB for embedded storage)
* `Consistency`: The writes will be acknowledged by the majority of the nodes and reads only served by the nodes that are up to date. Using Raft and BadgerDB we can provide a snapshot consistency. 
* `Easy to use API` DejaQ provides a Swagger/OpenAPI HTTP Rest API.
* `Dynamic consuming` One of the largest advantages of DejaQ is that topics are split into partitions (internally) and they are assigned dynamically at runtime depending on the active consumers. Although the algorithm in this alpha version is basic it will be extended in the future to allow more complex consuming strategies.
* `At least once delivery` with the best effort for exactly-once-delivery with the warning that the consumer has to successfully acknowledge the processed messages in a timely manner (as long as it has its partitions assigned).
* `Ordering` is guaranteed at the consumer level (it will receive the messages with the smallest priority at each pull from its assigned partitions). Different consumers may receive higher priority messages at the same time (because they consume from different partitions)
* `Shared nothing` architecture - all brokers can serve all purposes (and if not they will act as a proxy)
* `Easy deployment`: All you need to run is a single binary that will contain all the dependencies and logic. We also provide Docker images.
* `Cloud native` we have docker images and binaries build pipelines, offer support for K8S deployments and export Prometheus compatible metrics. Basically anything you need to run DejaQ in an elastic distributed nature.  
* `Agnostic` messages, for DejaQ the messages are just blob of bytes.
* `Concurrency` can be achieved by connecting _any_ number of producers and consumers with any broker

We have a few hard-limits like 1M maximum number of active consumers on a topic, a maximum message size of 64kb and more. See [limits.go](./storage/limits.go) for more details.


# Roadmap
## Alpha2 (next iteration)

* Cooperative dynamic consuming: Consumers are assigned priorities per partitions (multiple consumers can consume same lagging partitions)
* TODO decide between Horizontal Scale vs Timeline Topics

## Beta and beyond
* `GRPC + flatbuffers` protocol for high throughput systems with `Push` messages capabilities
* `RBAC` minimal system (APIKeys with rights on topics)
* Brokers auto-discovery 
* Leader brokers (that have the cluster metadata OR external ETCD cluster)
* `Encryption`: on-transit and on-rest
* `Horizontal scale` (have partitions spread on nodes)
* `backup/restore` capabilities
* `large messages` - We believe that large messages >64kb should live in distributed shared file systems and not in messaging queues databases so our broker will support block disk/persistent volumes (like AWS EBS) and Object Storage (like AWS S3) that will do the heavy lifting for us.

Timeline topic will bring time-scheduled messages with a custom set of features (on top of PriorityQueue):
* `Timeline topic` (schedule message in the future where priority is the Unix TS + ability to cancel them)
* `Mutable` messages, custom message IDs, cancel messages
* `Producers` can specify the partition (to allow consumers an ordered based processing)


# Contributing 

See https://github.com/dejaq/broker/wiki/Contributing and choose your poison: https://github.com/dejaq/broker/issues

# Feedback

Can be provided here on Github or at team at dejaq.io
