/// ## Apache Kafka client library for Dartlang
///
/// This library implements Kafka binary protocol and provides
/// high-level abstractions for producing and consuming messages.
library kafka;

import 'dart:async';
import 'dart:collection';
import 'dart:io';

import 'package:quiver/collection.dart';

import 'common.dart';

import 'protocol.dart';

export 'common.dart' hide groupBy, kafkaLogger;
export 'protocol.dart' show TopicMetadata;

part 'src/consumer.dart';
part 'src/consumer_group.dart';
part 'src/fetcher.dart';
part 'src/offset_master.dart';
part 'src/producer.dart';
part 'src/session.dart';
