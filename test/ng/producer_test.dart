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

    // test('it can produce messages to multiple brokers', () async {
    //   var producer = new Producer(_session, 1, 100);
    //   var result = await producer.produce([
    //     new ProduceEnvelope(_topicName, 0, [new Message('test1'.codeUnits)]),
    //     new ProduceEnvelope(_topicName, 1, [new Message('test2'.codeUnits)]),
    //     new ProduceEnvelope(_topicName, 2, [new Message('test3'.codeUnits)]),
    //   ]);
    //   expect(result.offsets[_topicName][0], greaterThanOrEqualTo(0));
    //   expect(result.offsets[_topicName][1], greaterThanOrEqualTo(0));
    //   expect(result.offsets[_topicName][2], greaterThanOrEqualTo(0));
    // });

    // test(
    //     'compression can not be set on individual messages in produce envelope',
    //     () {
    //   expect(() {
    //     new ProduceEnvelope('test', 0, [
    //       new Message([1],
    //           attributes: new MessageAttributes(KafkaCompression.gzip))
    //     ]);
    //   }, throwsStateError);
    // });
  });
}
