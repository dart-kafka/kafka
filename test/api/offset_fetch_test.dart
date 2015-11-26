library kafka.test.api.offset_fetch;

import 'package:test/test.dart';
import 'package:kafka/kafka.dart';
import 'package:kafka/protocol.dart';
import '../setup.dart';

void main() {
  group('OffsetFetchApi:', () {
    KafkaSession _session;
    OffsetFetchRequest _request;
    Broker _coordinator;
    String _testGroup;

    setUp(() async {
      var ip = await getDefaultHost();
      _session = new KafkaSession([new ContactPoint(ip, 9092)]);
      var now = new DateTime.now();
      _testGroup = 'group:' + now.millisecondsSinceEpoch.toString();
      var metadata = await _session.getConsumerMetadata(_testGroup);
      _coordinator = metadata.coordinator;
      _request = new OffsetFetchRequest(_testGroup, {
        'dartKafkaTest': new Set.from([0])
      });
    });

    tearDown(() async {
      await _session.close();
    });

    test('it fetches consumer offsets', () async {
      OffsetFetchResponse response =
          await _session.send(_coordinator, _request);
      expect(response.offsets, hasLength(equals(1)));
      expect(response.offsets.first.topicName, equals('dartKafkaTest'));
      expect(response.offsets.first.partitionId, equals(0));
      expect(response.offsets.first.errorCode, equals(0));
    });
  });
}
