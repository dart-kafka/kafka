import 'package:test/test.dart';
import 'package:kafka/ng.dart';

void main() {
  group('OffsetFetchApi:', () {
    KSession _session;
    OffsetFetchRequestV1 _request;
    Broker _coordinator;
    String _testGroup;

    setUp(() async {
      _session =
          new KSession(contactPoints: [new ContactPoint('127.0.0.1:9092')]);
      var now = new DateTime.now();
      var metadata = new KMetadata(session: _session);
      _testGroup = 'group:' + now.millisecondsSinceEpoch.toString();
      _coordinator = await metadata.fetchGroupCoordinator(_testGroup);
      _request = new OffsetFetchRequestV1(_testGroup, {
        'dartKafkaTest': new Set.from([0])
      });
    });

    tearDown(() async {
      await _session.close();
    });

    test('it fetches consumer offsets', () async {
      OffsetFetchResponseV1 response =
          await _session.send(_request, _coordinator.host, _coordinator.port);
      expect(response.offsets, hasLength(equals(1)));
      expect(response.offsets.first.topicName, equals('dartKafkaTest'));
      expect(response.offsets.first.partitionId, equals(0));
      expect(response.offsets.first.errorCode, equals(0));
    });
  });
}
