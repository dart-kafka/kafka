/// ## Library providing interface to Apache Kafka.
///
/// This library implements Kafka binary protocol as well as provides some
/// high-level abstractions for producing and consuming messages.
library kafka;

import 'dart:async';
import 'dart:collection';
import 'dart:io';

import 'src/protocol.dart';

export 'src/protocol.dart';

part 'src/common.dart';
part 'src/consumer.dart';
part 'src/consumer_group.dart';
part 'src/errors.dart';
part 'src/fetcher.dart';
part 'src/offset_master.dart';
part 'src/producer.dart';
part 'src/session.dart';
