library kafka.common.errors.test;

import 'package:test/test.dart';
import 'package:kafka/common.dart';

void main() {
  group('KafkaServerError:', () {
    test('it can be converted to string', () {
      expect(new KafkaServerError.fromCode(0, null).toString(),
          'KafkaServerError: NoError(0)');
    });
  });
}
