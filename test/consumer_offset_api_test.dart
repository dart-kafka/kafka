import 'package:test/test.dart';
import 'package:kafka/kafka.dart';

void main() {
  group('OffsetFetchApi:', () {
    Session session = new Session(['127.0.0.1:9092']);
    OffsetFetchRequest _request;
    Broker _coordinator;
    String _testGroup;

    setUp(() async {
      var now = new DateTime.now();
      _testGroup = 'group:' + now.millisecondsSinceEpoch.toString();
      _coordinator = await session.metadata.fetchGroupCoordinator(_testGroup);
      _request = new OffsetFetchRequest(
          _testGroup, [new TopicPartition('dartKafkaTest', 0)]);
    });

    tearDownAll(() async {
      await session.close();
    });

    test('it fetches consumer offsets', () async {
      OffsetFetchResponse response =
          await session.send(_request, _coordinator.host, _coordinator.port);
      expect(response.offsets, hasLength(equals(1)));
      expect(response.offsets.first.topic, equals('dartKafkaTest'));
      expect(response.offsets.first.partition, equals(0));
      expect(response.offsets.first.error, equals(0));
    });
  });
}
