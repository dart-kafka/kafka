library kafka.test.api.offset_commit;

import 'package:test/test.dart';
import 'package:kafka/kafka.dart';
import 'package:kafka/protocol.dart';
import '../setup.dart';

void main() {
  group('OffsetCommitApi', () {
    String _topicName = 'dartKafkaTest';
    KafkaSession _session;
    Broker _host;
    Broker _coordinator;
    int _offset;
    String _testGroup;

    setUp(() async {
      var ip = await getDefaultHost();
      _session = new KafkaSession([new ContactPoint(ip, 9092)]);
      var meta = await _session.getMetadata();
      var leaderId = meta.getTopicMetadata(_topicName).getPartition(0).leader;
      _host = meta.getBroker(leaderId);

      ProduceRequest produce = new ProduceRequest(1, 1000);
      var now = new DateTime.now();
      var message = 'test:' + now.toIso8601String();
      produce.addMessages(_topicName, 0, [new Message(message.codeUnits)]);
      var response = await _session.send(_host, produce);
      _offset = response.topics.first.partitions.first.offset;

      _testGroup = 'group:' + now.millisecondsSinceEpoch.toString();
      var metadata = await _session.getConsumerMetadata(_testGroup);
      _coordinator = metadata.coordinator;
    });

    tearDown(() async {
      await _session.close();
    });

    test('it commits consumer offsets', () async {
      var offsets = new Map<String, List<ConsumerOffset>>();
      offsets['dartKafkaTest'] = [new ConsumerOffset(0, _offset, 'helloworld')];

      var request = new OffsetCommitRequest(_testGroup, offsets, 0, '');

      var response = await _session.send(_coordinator, request);
      expect(response.topics, hasLength(equals(1)));
      expect(response.topics, contains('dartKafkaTest'));
      var partitions = response.topics['dartKafkaTest'];
      expect(partitions, hasLength(1));
      var p = partitions.first;
      expect(p.errorCode, equals(0));

      var fetch = new OffsetFetchRequest(_testGroup, {
        _topicName: new Set.from([0])
      });

      var fetchResponse = await _session.send(_coordinator, fetch);
      var info = fetchResponse.offsets[_topicName].first;
      expect(info.errorCode, equals(0));
      expect(info.offset, equals(_offset));
      expect(info.metadata, equals('helloworld'));
    });
  });
}
