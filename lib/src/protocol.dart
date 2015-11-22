library kafka.protocol;

import 'dart:collection';
import 'dart:convert';
import 'dart:io';
import 'dart:math';
import 'dart:typed_data';

import 'package:logging/logging.dart';
import 'package:tuple/tuple.dart';

part 'api/consumer_metadata_api.dart';
part 'api/errors.dart';
part 'api/fetch_api.dart';
part 'api/messages.dart';
part 'api/metadata_api.dart';
part 'api/offset_api.dart';
part 'api/offset_commit_api.dart';
part 'api/offset_fetch_api.dart';
part 'api/produce_api.dart';
part 'protocol/bytes_builder.dart';
part 'protocol/bytes_reader.dart';
part 'protocol/common.dart';
part 'util/crc32.dart';

/// String identifier used to pass to Kafka server in API calls.
const String dartKafkaId = 'dart_kafka';

final Logger _logger = new Logger('Kafka');

/// Logger for this library.
///
/// Doesn't do anything by default. You should set log level and add your handler
/// in order to get logs.
Logger get kafkaLogger => _logger;
