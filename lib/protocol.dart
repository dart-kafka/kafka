/// Subpackage with implementation of Kafka protocol.
///
/// Users of this package are not supposed to import this library directly and
/// use main 'kafka' package instead.
library kafka.protocol;

import 'dart:collection';
import 'dart:io';
import 'dart:math';

import 'common.dart';
import 'src/io/bytes_reader.dart';
import 'src/io/bytes_builder.dart';
import 'errors.dart';

part 'src/protocol/common.dart';
part 'src/protocol/consumer_metadata_api.dart';
part 'src/protocol/fetch_api.dart';
part 'src/protocol/group_membership_api.dart';
part 'src/protocol/messages.dart';
part 'src/protocol/metadata_api.dart';
part 'src/protocol/offset_api.dart';
part 'src/protocol/offset_commit_api.dart';
part 'src/protocol/offset_fetch_api.dart';
part 'src/protocol/produce_api.dart';
part 'src/util/crc32.dart';
