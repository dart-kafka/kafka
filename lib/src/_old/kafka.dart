/// ## Apache Kafka client library for Dartlang
///
/// This library implements Kafka binary protocol and provides
/// high-level abstractions for producing and consuming messages.
library kafka;

import 'dart:async';
import 'dart:collection';
import 'dart:io';

import 'package:logging/logging.dart';
import 'package:quiver/collection.dart';

import 'common.dart';
import 'errors.dart';
import 'io/bytes_reader.dart';
import 'protocol.dart';

export 'common.dart' hide groupBy, kafkaLogger;
export 'protocol.dart' show TopicMetadata;

part 'consumer.dart';
part 'producer.dart';
part 'session.dart';
part 'high_level_consumer.dart';
