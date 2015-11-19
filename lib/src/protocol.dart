library kafka.protocol;

import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

part 'protocol/bytes_builder.dart';
part 'protocol/bytes_reader.dart';

/// String identifier used to pass to Kafka server in API calls.
const String dartKafkaId = 'dart_kafka-v0.1.0';
