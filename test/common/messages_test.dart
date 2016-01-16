library kafka.common.messages.test;

import 'package:test/test.dart';
import 'package:kafka/common.dart';

void main() {
  group('Messages:', () {
    test(
        'compression can not be set on individual messages in produce envelope',
        () {
      expect(() {
        new ProduceEnvelope('test', 0, [
          new Message([1],
              attributes: new MessageAttributes(KafkaCompression.gzip))
        ]);
      }, throwsStateError);
    });
  });

  group('MessageAttributes:', () {
    test('get compression from int', () {
      expect(KafkaCompression.none, MessageAttributes.getCompression(0));
      expect(KafkaCompression.gzip, MessageAttributes.getCompression(1));
      expect(KafkaCompression.snappy, MessageAttributes.getCompression(2));
    });

    test('convert to int', () {
      expect(new MessageAttributes(KafkaCompression.none).toInt(), equals(0));
      expect(new MessageAttributes(KafkaCompression.gzip).toInt(), equals(1));
      expect(new MessageAttributes(KafkaCompression.snappy).toInt(), equals(2));
    });
  });
}
