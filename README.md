# Dart Kafka

[![Build Status](https://travis-ci.org/pulyaevskiy/dart-kafka.svg?branch=master)](https://travis-ci.org/pulyaevskiy/dart-kafka)

Kafka client library written in Dart.

### Current status

This library is in it's early stages and is not ready for production deployments.
However APIs are starting to settle already.

This package is not published on Pub yet. Once APIs are stable enough I'm planning
to release first beta which will be uploaded to Pub as well.

But if you feel adventurous you can try it out already. You can find some examples below.

### Producer example

Producer supports "auto-discovery" of brokers for publishing messages.

```dart
import 'dart:io';
import 'package:kafka/kafka.dart';

main(List<String> arguments) async {
  var host = new KafkaHost('127.0.0.1', 9092);
  var client = new KafkaClient([host]);

  var producer = new Producer(client, 1, 1000);
  producer.addMessages('topicName', 0, [new Message('msgForPartition0'.codeUnits)]);
  producer.addMessages('topicName', 1, [new Message('msgForPartition1'.codeUnits)]);
  var response = await producer.send();
  print(response.hasErrors);
}
```

### Consumer example (with ConsumerGroup offset handling)

* Consumer also supports "auto-discovery" of brokers and it will start 1 worker per Kafka broker.
* Current implementation will auto-commit offsets after each message. There is plans to make this behavior configurable.

```dart
import 'dart:io';
import 'dart:async';
import 'package:kafka/kafka.dart';

void main(List<String> arguments) {
  run().then((_) => exit(0));
}

Future run() async {
  var completer = new Completer();
  var host = new KafkaHost('127.0.0.1', 9092);
  var client = new KafkaClient([host]);
  var group = new ConsumerGroup(client, 'consumerGroupName');
  var topics = {
    'topicName': [0, 1] // list of partitions to consume from.
  };

  var consumer = new Consumer(client, group, topics, 100, 1);
  await for (MessageEnvelope message in consumer.consume(limit: 5)) {
    var value = new String.fromCharCodes(message.message.value);
    print('Got message: ${message.offset}, ${value}');
    message.ack('metadata'); // This is required since we're committing offsets.
  }
  print('Done');
  completer.complete();

  return completer.future;
}
```

### Supported protocol versions

Current version targets version `0.8.2` of the Kafka protocol. There is no plans to support earlier versions.
