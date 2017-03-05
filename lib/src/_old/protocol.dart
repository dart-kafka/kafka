/// Subpackage with implementation of Kafka protocol.
///
/// Users of this package are not supposed to import this library directly and
/// use main 'kafka' package instead.
library kafka.protocol;

import 'dart:collection';
import 'dart:io';
import 'dart:math';

import 'common.dart';
import 'errors.dart';
import 'io/bytes_builder.dart';
import 'io/bytes_reader.dart';
import '../util/crc32.dart';

part 'protocol/common.dart';
part 'protocol/consumer_metadata_api.dart';
part 'protocol/fetch_api.dart';
part 'protocol/group_membership_api.dart';
part 'protocol/messages.dart';
part 'protocol/metadata_api.dart';
part 'protocol/offset_api.dart';
part 'protocol/offset_commit_api.dart';
part 'protocol/offset_fetch_api.dart';
part 'protocol/produce_api.dart';
