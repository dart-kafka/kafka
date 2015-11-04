library kafka.test.library;

import 'package:test/test.dart';
import 'package:logging/logging.dart';
import 'package:kafka/kafka.dart';

void main() {
  group('Library', () {
    setUp(() {});

    test('it has logger', () {
      expect(kafkaLogger, new isInstanceOf<Logger>());
      LogRecord r;
      kafkaLogger.onRecord.listen((record) {
        r = record;
      });
      kafkaLogger.info('Hello from DartKafka');
      expect(r, new isInstanceOf<LogRecord>());
    });
  });
}
