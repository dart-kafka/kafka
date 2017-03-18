# Dart Kafka

[![Build Status](https://travis-ci.org/dart-kafka/kafka.svg?branch=kafka-0.10)](https://travis-ci.org/dart-kafka/kafka)
[![Coverage](https://codecov.io/gh/pulyaevskiy/dart-kafka/branch/master/graph/badge.svg)](https://codecov.io/gh/pulyaevskiy/dart-kafka)
[![License](https://img.shields.io/badge/license-BSD--2-blue.svg)](https://raw.githubusercontent.com/pulyaevskiy/dart-kafka/master/LICENSE)

Kafka client library written in Dart.

### Things that are not supported yet.

* Snappy compression.
* SSL

## Installation

_To be updated with first release._


## Producer

Producer publishes messages to the Kafka cluster. Here is a simple example
of using the producer to send `String` records:

```dart
import 'dart:async';

import 'package:kafka/kafka.dart';

Future main() async {
  var config = new ProducerConfig(bootstrapServers: ['127.0.0.1:9092']);
  var producer = new Producer<String, String>(
      new StringSerializer(), new StringSerializer(), config);
  var record = new ProducerRecord('example-topic', 0, 'key', 'value');
  producer.add(record);
  var result = await record.result;
  print(result);
  await producer.close();
}
```

The producer implements `StreamSink` interface so it is possible to send
individual records via `add()` as well as streams of records via 
`addStream()`.

The producer buffers records internally so that they can be sent in bulk to
the server. This does not necessarily mean increased latency for record
delivery. When a record is added with `add()` it is sent immediately
(although an asynchronous gap is present between call to `add()` and actual
send) as long as there is available slot in IO pool. By default producer
has a limit of up to 5 in-flight requests per broker connection,
so delay can occur only if all the slots are already occupied.

## Note on new API design

Public API of this library has been completely re-written since original 
version (which supported only Kafka 0.8.x and was never published on Pub).

New design is trying to accomplish two main goals:

####  1. Follow official Java client semantics and contract.

`Producer` and `Consumer` are trying to preserve characteristics of
original Java implementations. This is why configurations for both
are almost identical to the ones in the official client. The way serialization is implemented also based on Java code.

#### 2. Streams-compatible public API.

The main reason is to allow better interoperability with other libraries.
`Producer` implements `StreamSink` for this specific reason, so instead of
having a `send()` method (as in Java client) there are `add()` and 
`addStream()`.

## Supported protocol versions

Current version targets version `0.10` of the Kafka protocol. 
There is no plans to support earlier versions.

## License

BSD-2
