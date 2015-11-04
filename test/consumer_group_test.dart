library kafka.test.consumer_group;

import 'package:test/test.dart';
import 'package:mockito/mockito.dart';
import 'package:kafka/kafka.dart';
import 'setup.dart';

void main() {
  group('ConsumerGroup', () {
    KafkaSession _session;
    String _topicName = 'dartKafkaTest';
    KafkaHost _coordinator;

    setUp(() async {
      var host = await getDefaultHost();
      var session = new KafkaSession([new KafkaHost(host, 9092)]);
      var metadata = await session.getConsumerMetadata('testGroup');
      _coordinator =
          new KafkaHost(metadata.coordinatorHost, metadata.coordinatorPort);

      _session = spy(new KafkaSessionMock(), session);
    });

    tearDown(() async {
      await _session.close();
    });

    test('it fetches offsets', () async {
      var group = new ConsumerGroup(_session, 'testGroup');
      var offsets = await group.fetchOffsets({
        _topicName: [0, 1, 2]
      });
      expect(offsets[_topicName].length, equals(3));
      offsets[_topicName].forEach((o) {
        expect(o.errorCode, 0);
      });
    });

    test('it tries to refresh coordinator host 3 times on fetchOffsets',
        () async {
      var badPort = (_coordinator.port == 9092) ? 9093 : 9092;
      when(_session.getConsumerMetadata('testGroup')).thenReturn(
          new ConsumerMetadataResponse(0, 1, _coordinator.host, badPort));

      var group = new ConsumerGroup(_session, 'testGroup');
      // Can't use expect(throws) here since it's async, so `verify` check below
      // fails.
      try {
        await group.fetchOffsets({
          _topicName: [0, 1, 2]
        });
      } catch (e) {
        expect(e, new isInstanceOf<KafkaApiError>());
        expect(e.errorCode, equals(16));
      }
      verify(_session.getConsumerMetadata('testGroup')).called(3);
    });

    test(
        'it retries to fetchOffsets 3 times if it gets OffsetLoadInProgress error',
        () async {
      var badOffsets = {
        _topicName: [
          new ConsumerOffset(0, -1, '', 14),
          new ConsumerOffset(1, -1, '', 14),
          new ConsumerOffset(2, -1, '', 14)
        ]
      };
      when(_session.send(argThat(new isInstanceOf<KafkaHost>()),
              argThat(new isInstanceOf<OffsetFetchRequest>())))
          .thenReturn(new OffsetFetchResponse.fromOffsets(badOffsets));

      var group = new ConsumerGroup(_session, 'testGroup');
      // Can't use expect(throws) here since it's async, so `verify` check below
      // fails.
      var now = new DateTime.now();
      try {
        await group.fetchOffsets({
          _topicName: [0, 1, 2]
        });
        fail('fetchOffsets must throw an error.');
      } catch (e) {
        var diff = now.difference(new DateTime.now());
        expect(diff.abs().inSeconds, greaterThanOrEqualTo(2));

        expect(e, new isInstanceOf<KafkaApiError>());
        expect(e.errorCode, equals(14));
      }
      verify(_session.send(argThat(new isInstanceOf<KafkaHost>()),
          argThat(new isInstanceOf<OffsetFetchRequest>()))).called(3);
    });

    test('it tries to refresh coordinator host 3 times on commitOffsets',
        () async {
      var badPort = (_coordinator.port == 9092) ? 9093 : 9092;
      when(_session.getConsumerMetadata('testGroup')).thenReturn(
          new ConsumerMetadataResponse(0, 1, _coordinator.host, badPort));

      var group = new ConsumerGroup(_session, 'testGroup');
      var offsets = new Map();
      offsets[_topicName] = [new ConsumerOffset(0, 3, '')];

      try {
        await group.commitOffsets(offsets, 0, 'test');
      } catch (e) {
        expect(e, new isInstanceOf<KafkaApiError>());
        expect(e.errorCode, equals(16));
      }
      verify(_session.getConsumerMetadata('testGroup')).called(3);
    });
  });
}

class KafkaSessionMock extends Mock implements KafkaSession {
  noSuchMethod(i) => super.noSuchMethod(i);
}
