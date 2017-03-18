import 'package:kafka/kafka.dart';
import 'package:test/test.dart';

void main() {
  group('OffsetApi:', () {
    String topic = 'dartKafkaTest';
    int partition;
    Broker broker;
    Session session = new Session(['127.0.0.1:9092']);
    int _offset;

    setUp(() async {
      var meta = await session.metadata.fetchTopics([topic]);
      var p = meta[topic].partitions[0];
      partition = p.id;
      var leaderId = p.leader;
      broker = meta.brokers[leaderId];

      var producer = new Producer(
          new StringSerializer(),
          new StringSerializer(),
          new ProducerConfig(bootstrapServers: ['127.0.0.1:9092']));
      var rec = new ProducerRecord(topic, partition, 'key', 'value');
      producer.add(rec);
      var result = await rec.result;
      _offset = result.offset;
      await producer.close();
    });

    tearDownAll(() async {
      await session.close();
    });

    test('it fetches offset info', () async {
      var request =
          new ListOffsetRequest({new TopicPartition(topic, partition): -1});
      ListOffsetResponse response =
          await session.send(request, broker.host, broker.port);

      expect(response.offsets, hasLength(1));
      var offset = response.offsets.first;
      expect(offset.error, equals(0));
      expect(offset.offset, equals(_offset + 1));
    });
  });
}
