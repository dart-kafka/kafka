# Dart Kafka

[![Build Status](https://img.shields.io/travis-ci/pulyaevskiy/dart-kafka.svg?branch=master&style=flat-square)](https://travis-ci.org/pulyaevskiy/dart-kafka)
[![Coverage Status](https://img.shields.io/coveralls/pulyaevskiy/dart-kafka.svg?branch=master&style=flat-square)](https://coveralls.io/github/pulyaevskiy/dart-kafka?branch=master)
[![License](https://img.shields.io/badge/license-BSD--2-blue.svg?style=flat-square)](https://raw.githubusercontent.com/pulyaevskiy/dart-kafka/master/LICENSE)

Kafka client library written in Dart.

### Current status

This library has not been used on production yet.

This package is not published on Pub yet but you can use git dependency in your
`pubspec.yaml`.

### Producer example

Producer supports "auto-discovery" of brokers for publishing messages.

```dart
// file:produce.dart
import 'dart:io';
import 'package:kafka/kafka.dart';

main(List<String> arguments) async {
  var host = new KafkaHost('127.0.0.1', 9092);
  var session = new KafkaSession([host]);

  var producer = new Producer(session, 1, 1000);
  var result = producer.produce([
    new ProduceEnvelope('topicName', 0, [new Message('msgForPartition0'.codeUnits)]),
    new ProduceEnvelope('topicName', 1, [new Message('msgForPartition1'.codeUnits)])
  ]);
  print(response.hasErrors);
  print(response.offsets);
  session.close(); // make sure to always close the session when the work is done.
}
```

Result:

```bash
$ dart produce.dart
$ false
$ {dartKafkaTest: {0: 213075, 1: 201680}}
```

### Consumer example (with ConsumerGroup offset handling)

* Consumer also supports "auto-discovery" of brokers and it will start 1 worker
  per Kafka broker.
* Each message in the Consumer stream is wrapped in `MessageEnvelope` which
  provides following methods:
  * `commit(String metadata)` - signals to worker that message has been processed
    and the offset should be committed.
  * `ack()` - signals to worker that message has been processed and we are ready
    for the next one.
  * `cancel()` - signals to cancel any further deliveries and close the stream.
    Note that offset of current message will not be committed in this case!
* Either `commit()`, `ack()` (or `cancel()`) method on each `MessageEnvelope` must be
  called in order for consumer to proceed to the next one in the stream.
* It is possible to configure Consumer behavior when it receives `OffsetOutOfRange`
  API error. Supported strategies: `resetToEarliest (default)`, `resetToLatest`,
  `throwError`. See `Consumer.onOffsetOutOfRange` property for details.

```dart
import 'dart:io';
import 'dart:async';
import 'package:kafka/kafka.dart';

void main(List<String> arguments) async {
  var host = new KafkaHost('127.0.0.1', 9092);
  var session = new KafkaSession([host]);
  var group = new ConsumerGroup(session, 'consumerGroupName');
  var topics = {
    'topicName': [0, 1] // list of partitions to consume from.
  };

  var consumer = new Consumer(session, group, topics, 100, 1);
  await for (MessageEnvelope envelope in consumer.consume(limit: 5)) {
    var value = new String.fromCharCodes(envelope.message.value);
    print('Got message: ${envelope.offset}, ${value}');
    envelope.commit('metadata'); // This is required.
  }
  session.close(); // make sure to always close the session when the work is done.
}
```

### Supported protocol versions

Current version targets version `0.8.2` of the Kafka protocol. There is no plans to support earlier versions.

### License

BSD-2
