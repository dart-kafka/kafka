import 'package:test/test.dart';
import 'package:kafka/ng.dart';

void main() {
  group('KafkaServerError:', () {
    test('it can be converted to string', () {
      expect(new KafkaServerError.fromCode(0, null).toString(), 'NoError(0)');
    });
  });
}
