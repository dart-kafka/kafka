// TODO: move to consumer_offset_api_test.dart
import 'package:test/test.dart';
import 'package:kafka/ng.dart';

void main() {
  group('OffsetCommitApi:', () {
    String _topicName = 'dartKafkaTest';
    KSession session = new KSession([new ContactPoint('127.0.0.1:9092')]);
    Metadata metadata = new Metadata(session);
    Broker coordinator;
    int _offset;
    String testGroup;

    tearDownAll(() async {
      await session.close();
    });

    setUp(() async {
      var producer = new KProducer(
          new StringSerializer(), new StringSerializer(), session);
      var result =
          await producer.send(new ProducerRecord(_topicName, 0, 'a', 'b'));

      _offset = result.offset;
      var date = new DateTime.now();
      testGroup = 'group:' + date.millisecondsSinceEpoch.toString();
      coordinator = await metadata.fetchGroupCoordinator(testGroup);
    });

    tearDownAll(() async {
      await session.close();
    });

    test('it commits consumer offsets', () async {
      var offsets = [new ConsumerOffset(_topicName, 0, _offset, 'helloworld')];

      OffsetCommitRequestV2 request =
          new OffsetCommitRequestV2(testGroup, offsets, -1, '', -1);

      OffsetCommitResponseV2 response =
          await session.send(request, coordinator.host, coordinator.port);
      expect(response.results, hasLength(equals(1)));
      expect(response.results.first.topic, equals(_topicName));
      expect(response.results.first.errorCode, equals(0));

      var fetch = new OffsetFetchRequest(testGroup, {
        _topicName: [0]
      });

      OffsetFetchResponse fetchResponse =
          await session.send(fetch, coordinator.host, coordinator.port);
      var offset = fetchResponse.offsets.first;
      expect(offset.error, equals(0));
      expect(offset.offset, equals(_offset));
      expect(offset.metadata, equals('helloworld'));
    });
  });
}
