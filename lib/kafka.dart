/// ## Library providing interface to Apache Kafka.
///
/// This library implementas Kafka binary protocal as well as provides some
/// high-level abstraction for producing and consuming messages.
library kafka;

import 'dart:async';
import 'dart:collection';
import 'dart:convert';
import 'dart:io';
import 'dart:math';
import 'dart:typed_data';

import 'package:logging/logging.dart';

part 'src/api/consumer_metadata_api.dart';
part 'src/api/errors.dart';
part 'src/api/fetch_api.dart';
part 'src/api/messages.dart';
part 'src/api/metadata_api.dart';
part 'src/api/offset_api.dart';
part 'src/api/offset_commit_api.dart';
part 'src/api/offset_fetch_api.dart';
part 'src/api/produce_api.dart';
part 'src/bytes_builder.dart';
part 'src/bytes_reader.dart';
part 'src/common.dart';
part 'src/consumer.dart';
part 'src/consumer_group.dart';
part 'src/errors.dart';
part 'src/offset_master.dart';
part 'src/producer.dart';
part 'src/session.dart';
part 'src/util/crc32.dart';
part 'src/util/tuple.dart';

/// String identifier of this library used to pass to Kafka server in API calls.
const String dartKafkaId = 'dart-kafka-v1.0.0-dev';

final Logger _logger = new Logger('Kafka');

/// Logger for this library.
///
/// Doesn't do anything by default. You should set log level and add your handler
/// in order to get logs.
Logger get kafkaLogger => _logger;
