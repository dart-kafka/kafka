library kafka.test.api.offset_commit;

import 'package:test/test.dart';
import 'package:kafka/kafka.dart';
import '../setup.dart';

void main() {
  group('OffsetCommitApi', () {
    String _topicName = 'dartKafkaTest';
    KafkaSession _session;
    KafkaHost _host;
    KafkaHost _coordinatorHost;
    int _offset;
    String _testGroup;

    setUp(() async {
      var ip = await getDefaultHost();
      _host = new KafkaHost(ip, 9092);
      _session = new KafkaSession([_host]);

      ProduceRequest produce = new ProduceRequest(1, 1000);
      var now = new DateTime.now();
      var message = 'test:' + now.toIso8601String();
      produce.addMessages(_topicName, 0, [new Message(message.codeUnits)]);
      var response = await _session.send(_host, produce);
      _offset = response.topics.first.partitions.first.offset;

      _testGroup = 'group:' + now.millisecondsSinceEpoch.toString();
      var metadata = await _session.getConsumerMetadata(_testGroup);
      _coordinatorHost =
          new KafkaHost(metadata.coordinatorHost, metadata.coordinatorPort);
    });

    tearDown(() async {
      await _session.close();
    });

    test('it commits consumer offsets', () async {
      var offsets = new Map<String, List<ConsumerOffset>>();
      offsets['dartKafkaTest'] = [new ConsumerOffset(0, _offset, 'helloworld')];

      var request = new OffsetCommitRequest(_testGroup, offsets, 0, '');

      var response = await _session.send(_coordinatorHost, request);
      expect(response.topics, hasLength(equals(1)));
      expect(response.topics, contains('dartKafkaTest'));
      var partitions = response.topics['dartKafkaTest'];
      expect(partitions, hasLength(1));
      var p = partitions.first;
      expect(p.errorCode, equals(0));

      var fetch = new OffsetFetchRequest(_testGroup, {
        _topicName: new Set.from([0])
      });

      var fetchResponse = await _session.send(_coordinatorHost, fetch);
      var info = fetchResponse.offsets[_topicName].first;
      expect(info.errorCode, equals(0));
      expect(info.offset, equals(_offset));
      expect(info.metadata, equals('helloworld'));
    });
  });
}
