import 'package:test/test.dart';
import 'package:kafka/kafka.dart';

void main() {
  group('KProducer:', () {
    var producer = new Producer<String, String>(
        new StringSerializer(),
        new StringSerializer(),
        new ProducerConfig(bootstrapServers: ['127.0.0.1:9092']));

    tearDownAll(() async {
      await producer.close();
    });

    test('it can produce messages to Kafka', () async {
      var rec = new ProducerRecord('testProduce', 0, 'key', 'value');
      producer.add(rec);
      var result = await rec.result;
      expect(result, isA<ProduceResult>());
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
  });
}
