import 'package:test/test.dart';
import 'package:kafka/ng.dart';

void main() {
  group('OffsetFetchApi:', () {
    KSession session = new KSession([new ContactPoint('127.0.0.1:9092')]);
    OffsetFetchRequestV1 _request;
    Broker _coordinator;
    String _testGroup;

    setUp(() async {
      var now = new DateTime.now();
      var metadata = new KMetadata(session);
      _testGroup = 'group:' + now.millisecondsSinceEpoch.toString();
      _coordinator = await metadata.fetchGroupCoordinator(_testGroup);
      _request = new OffsetFetchRequestV1(_testGroup, {
        'dartKafkaTest': [0]
      });
    });

    tearDownAll(() async {
      await session.close();
    });

    test('it fetches consumer offsets', () async {
      OffsetFetchResponseV1 response =
          await session.send(_request, _coordinator.host, _coordinator.port);
      expect(response.offsets, hasLength(equals(1)));
      expect(response.offsets.first.topic, equals('dartKafkaTest'));
      expect(response.offsets.first.partition, equals(0));
      expect(response.offsets.first.errorCode, equals(0));
    });
  });
}
