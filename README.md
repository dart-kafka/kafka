# Dart Kafka

[![Build Status](https://travis-ci.org/pulyaevskiy/dart-kafka.svg?branch=master)](https://travis-ci.org/pulyaevskiy/dart-kafka)
[![Coverage](https://codecov.io/gh/pulyaevskiy/dart-kafka/branch/master/graph/badge.svg)](https://codecov.io/gh/pulyaevskiy/dart-kafka)
[![License](https://img.shields.io/badge/license-BSD--2-blue.svg)](https://raw.githubusercontent.com/pulyaevskiy/dart-kafka/master/LICENSE)

Kafka client library written in Dart.

### Current status

This library is a work-in-progress and has not been used in production yet.

### Things that are not supported yet.

* Snappy compression.

## Installation

To be updated with first release.

## Producer API

New producer API is trying to follow interface of official Java client library:

```dart
import 'package:kafka/kafka.dart';

void main() {
  var producer = new Producer<String, String>(
      new StringSerializer(), new StringSerializer(), session);
  var result = await producer
      .send(new ProducerRecord('testProduce', 0, 'key', 'value'));
}
```

## Supported protocol versions

Current version targets version `0.10` of the Kafka protocol. There is no plans
to support earlier versions.

## License

BSD-2
