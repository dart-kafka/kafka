import 'package:kafka/ng.dart';
import 'package:test/test.dart';

void main() {
  group('OffsetApi:', () {
    String topic = 'dartKafkaTest';
    int partition;
    Broker broker;
    Session session = new Session([new ContactPoint('127.0.0.1:9092')]);
    Metadata metadata = new Metadata(session);
    int _offset;

    setUp(() async {
      var meta = await metadata.fetchTopics([topic]);
      var p = meta.first.partitions.first;
      partition = p.id;
      var leaderId = p.leader;
      var brokers = await metadata.listBrokers();
      broker = brokers.firstWhere((_) => _.id == leaderId);

      var producer =
          new Producer(new StringSerializer(), new StringSerializer(), session);
      var result = await producer
          .send(new ProducerRecord(topic, partition, 'key', 'value'));
      _offset = result.offset;
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
