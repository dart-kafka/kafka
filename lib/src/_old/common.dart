/// Common dependencies for other Kafka libraries withing this package.
library kafka.common;

import 'package:logging/logging.dart';

part 'common/messages.dart';
part 'common/metadata.dart';
part 'common/offsets.dart';

/// Logger for this library.
///
/// Doesn't do anything by default. You should set log level and add your handler
/// in order to get logs.
final Logger kafkaLogger = new Logger('Kafka');
