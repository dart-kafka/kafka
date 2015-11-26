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
}
