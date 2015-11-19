# Getting started with Dart Kafka client

This library provides several high-level API objects to interact with Kafka:

* Producer - publishes messages to Kafka topics
* Consumer - consumes messages from Kafka topics and stores it's state (current
  offsets). Leverages ConsumerMetadata API via ConsumerGroup.
* Fetcher - consumes messages from Kafka without storing state.
* OffsetMaster - provides convenience on top of Offset API allowing to easily
  retrieve earliest and latest offsets of particular topic-partitions.
* ConsumerGroup - provides convenience on top of Consumer Metadata API to easily
  fetch or commit consumer offsets.
