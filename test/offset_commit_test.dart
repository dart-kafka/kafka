// TODO: move to consumer_offset_api_test.dart
import 'package:kafka/kafka.dart';
import 'package:test/test.dart';

void main() {
  group('OffsetCommitApi:', () {
    String _topic = 'dartKafkaTest';
    Session session = new Session(['127.0.0.1:9092']);
    Broker coordinator;
    int _offset;
    String testGroup;

    tearDownAll(() async {
      await session.close();
    });

    setUp(() async {
      var producer = new Producer(
          new StringSerializer(),
          new StringSerializer(),
          new ProducerConfig(bootstrapServers: ['127.0.0.1:9092']));
      var rec = new ProducerRecord(_topic, 0, 'a', 'b');
      producer.add(rec);
      var result = await rec.result;

      _offset = result.offset;
      var date = new DateTime.now();
      testGroup = 'group:' + date.millisecondsSinceEpoch.toString();
      coordinator = await session.metadata.fetchGroupCoordinator(testGroup);
      await producer.close();
    });

    tearDownAll(() async {
      await session.close();
    });

    test('it commits consumer offsets', () async {
      var offsets = [new ConsumerOffset(_topic, 0, _offset, 'helloworld')];

      OffsetCommitRequest request =
          new OffsetCommitRequest(testGroup, offsets, -1, '', -1);

      OffsetCommitResponse response =
          await session.send(request, coordinator.host, coordinator.port);
      expect(response.results, hasLength(equals(1)));
      expect(response.results.first.topic, equals(_topic));
      expect(response.results.first.error, equals(0));

      var fetch =
          new OffsetFetchRequest(testGroup, [new TopicPartition(_topic, 0)]);

      OffsetFetchResponse fetchResponse =
          await session.send(fetch, coordinator.host, coordinator.port);
      var offset = fetchResponse.offsets.first;
      expect(offset.error, equals(0));
      expect(offset.offset, equals(_offset));
      expect(offset.metadata, equals('helloworld'));
    });
  });
}
