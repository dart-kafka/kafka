import 'package:test/test.dart';
import 'package:kafka/ng.dart';

void main() {
  group('KProducer:', () {
    KSession session = new KSession([new ContactPoint('127.0.0.1:9092')]);

    tearDownAll(() async {
      await session.close();
    });

    test('it can produce messages to Kafka', () async {
      var producer = new KProducer<String, String>(
          new StringSerializer(), new StringSerializer(), session);
      var result = await producer
          .send(new ProducerRecord('testProduce', 0, 'key', 'value'));
      expect(result, new isInstanceOf<ProduceResult>());
      expect(result.topicPartition, new TopicPartition('testProduce', 0));
      expect(result.offset, greaterThanOrEqualTo(0));
    });
  });
}
