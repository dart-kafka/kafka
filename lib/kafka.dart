/// ## Library providing interface to Apache Kafka.
///
/// This library implementas Kafka binary protocal as well as provides some
/// high-level abstraction for producing and consuming messages.
library kafka;

import 'dart:io';
import 'dart:async';
import 'dart:convert';
import 'dart:math';
import 'dart:collection';
import 'dart:typed_data';
import 'package:logging/logging.dart';

part 'src/util/crc32.dart';
part 'src/util/tuple.dart';

part 'src/common.dart';
part 'src/errors.dart';
part 'src/config.dart';
part 'src/bytes_builder.dart';
part 'src/bytes_reader.dart';
part 'src/client.dart';
part 'src/api/errors.dart';
part 'src/api/messages.dart';
part 'src/api/metadata_api.dart';
part 'src/api/produce_api.dart';
part 'src/api/fetch_api.dart';
part 'src/api/offset_api.dart';
part 'src/api/consumer_metadata_api.dart';
part 'src/api/offset_fetch_api.dart';
part 'src/api/offset_commit_api.dart';
part 'src/producer.dart';
part 'src/consumer.dart';
part 'src/consumer_group.dart';

/// String identifier of this library used to pass to Kafka server in API calls.
const String kafkaClientId = 'dart-kafka-v1.0.0-dev';

/// Logger for this library.
///
/// Doesn't do anything by default. You should set log level and add your handler
/// in order to get logs.
final Logger logger = new Logger('Kafka');
