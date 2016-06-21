library kafka.test.api.offset_commit;

import 'package:test/test.dart';
import 'package:kafka/kafka.dart';
import 'package:kafka/protocol.dart';
import '../setup.dart';

void main() {
  group('OffsetCommitApi:', () {
    String _topicName = 'dartKafkaTest';
    KafkaSession _session;
    Broker _host;
    Broker _coordinator;
    int _offset;
    String _testGroup;

    setUp(() async {
      var ip = await getDefaultHost();
      _session = new KafkaSession([new ContactPoint(ip, 9092)]);
      var meta = await _session.getMetadata([_topicName].toSet());
      var leaderId = meta.getTopicMetadata(_topicName).getPartition(0).leader;
      _host = meta.getBroker(leaderId);

      var now = new DateTime.now();
      var message = 'test:' + now.toIso8601String();
      ProduceRequest produce = new ProduceRequest(1, 1000, [
        new ProduceEnvelope(_topicName, 0, [new Message(message.codeUnits)])
      ]);
      ProduceResponse response = await _session.send(_host, produce);
      _offset = response.results.first.offset;

      _testGroup = 'group:' + now.millisecondsSinceEpoch.toString();
      var metadata = await _session.getConsumerMetadata(_testGroup);
      _coordinator = metadata.coordinator;
    });

    tearDown(() async {
      await _session.close();
    });

    test('it commits consumer offsets', () async {
      var offsets = [
        new ConsumerOffset('dartKafkaTest', 0, _offset, 'helloworld')
      ];

      var request = new OffsetCommitRequest(_testGroup, offsets, -1, '', -1);

      OffsetCommitResponse response =
          await _session.send(_coordinator, request);
      expect(response.offsets, hasLength(equals(1)));
      expect(response.offsets.first.topicName, equals('dartKafkaTest'));
      expect(response.offsets.first.errorCode, equals(0));

      var fetch = new OffsetFetchRequest(_testGroup, {
        _topicName: new Set.from([0])
      });

      OffsetFetchResponse fetchResponse =
          await _session.send(_coordinator, fetch);
      var offset = fetchResponse.offsets.first;
      expect(offset.errorCode, equals(0));
      expect(offset.offset, equals(_offset));
      expect(offset.metadata, equals('helloworld'));
    });
  });
}
