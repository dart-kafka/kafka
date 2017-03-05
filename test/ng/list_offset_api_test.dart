import 'package:kafka/ng.dart';
import 'package:test/test.dart';

void main() {
  group('OffsetApi:', () {
    String topic = 'dartKafkaTest';
    int partitionId;
    Broker broker;
    KSession session = new KSession([new ContactPoint('127.0.0.1:9092')]);
    Metadata metadata = new Metadata(session);
    int _offset;

    setUp(() async {
      var meta = await metadata.fetchTopics([topic]);
      var p = meta.first.partitions.first;
      partitionId = p.id;
      var leaderId = p.leader;
      var brokers = await metadata.listBrokers();
      broker = brokers.firstWhere((_) => _.id == leaderId);

      var producer =
          new Producer(new StringSerializer(), new StringSerializer(), session);
      var result = await producer
          .send(new ProducerRecord(topic, partitionId, 'key', 'value'));
      _offset = result.offset;
    });

    tearDownAll(() async {
      await session.close();
    });

    test('it fetches offset info', () async {
      var request =
          new ListOffsetRequest({new TopicPartition(topic, partitionId): -1});
      ListOffsetResponse response =
          await session.send(request, broker.host, broker.port);

      expect(response.offsets, hasLength(1));
      var offset = response.offsets.first;
      expect(offset.errorCode, equals(0));
      expect(offset.offset, equals(_offset + 1));
    });
  });
}
