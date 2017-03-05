import 'package:test/test.dart';
import 'package:kafka/ng.dart';

void main() {
  group('Errors:', () {
    test('it can be converted to string', () {
      expect(new KafkaError.fromCode(1, null).toString(),
          'OffsetOutOfRangeError(1)');
    });
  });
}
