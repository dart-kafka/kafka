library kafka.test.consumer_group;

import 'package:test/test.dart';
import 'package:mockito/mockito.dart';
import 'package:kafka/kafka.dart';
import 'package:kafka/protocol.dart';
import 'setup.dart';
import 'dart:async';

class KafkaSessionMock extends Mock implements KafkaSession {}

void main() {
  group('ConsumerGroup:', () {
    KafkaSession _session;
    String _topicName = 'dartKafkaTest';
    Broker _coordinator;
    Broker _badCoordinator;

    setUp(() async {
      var host = await getDefaultHost();
      var session = new KafkaSession([new ContactPoint(host, 9092)]);
      var brokersMetadata = await session.getMetadata([_topicName].toSet());

      var metadata = await session.getConsumerMetadata('testGroup');
      await session.close();
      _coordinator = metadata.coordinator;
      _badCoordinator =
          brokersMetadata.brokers.firstWhere((b) => b.id != _coordinator.id);
      _session = spy(new KafkaSessionMock(),
          new KafkaSession([new ContactPoint(host, 9092)]));
    });

    tearDown(() async {
      await _session.close();
    });

    test('it fetches offsets', () async {
      var group = new ConsumerGroup(_session, 'testGroup');
      var offsets = await group.fetchOffsets({
        _topicName: [0, 1, 2].toSet()
      });
      expect(offsets.length, equals(3));
      offsets.forEach((o) {
        expect(o.errorCode, 0);
      });
    });

    test('it tries to refresh coordinator host 3 times on fetchOffsets',
        () async {
      when(_session.getConsumerMetadata('testGroup')).thenReturn(
          new Future.value(new GroupCoordinatorResponse(0, _badCoordinator.id,
              _badCoordinator.host, _badCoordinator.port)));

      var group = new ConsumerGroup(_session, 'testGroup');
      // Can't use expect(throws) here since it's async, so `verify` check below
      // fails.
      try {
        await group.fetchOffsets({
          _topicName: [0, 1, 2].toSet()
        });
      } catch (e) {
        expect(e, new isInstanceOf<KafkaServerError>());
        expect(e.code, equals(16));
      }
      verify(_session.getConsumerMetadata('testGroup')).called(3);
    });

    test(
        'it retries to fetchOffsets 3 times if it gets OffsetLoadInProgress error',
        () async {
      var badOffsets = [
        new ConsumerOffset(_topicName, 0, -1, '', 14),
        new ConsumerOffset(_topicName, 1, -1, '', 14),
        new ConsumerOffset(_topicName, 2, -1, '', 14)
      ];
      when(_session.send(argThat(new isInstanceOf<Broker>()),
              argThat(new isInstanceOf<OffsetFetchRequest>())))
          .thenAnswer((invocation) {
        throw new KafkaServerError.fromCode(
            KafkaServerError.OffsetsLoadInProgress,
            new OffsetFetchResponse.fromOffsets(badOffsets));
      });

      var group = new ConsumerGroup(_session, 'testGroup');
      // Can't use expect(throws) here since it's async, so `verify` check below
      // fails.
      var now = new DateTime.now();
      try {
        await group.fetchOffsets({
          _topicName: [0, 1, 2].toSet()
        });
        fail('fetchOffsets must throw an error.');
      } catch (e) {
        var diff = now.difference(new DateTime.now());
        expect(diff.abs().inSeconds, greaterThanOrEqualTo(2));

        expect(e, new isInstanceOf<KafkaServerError>());
        expect(e.code, equals(14));
      }
      verify(_session.send(argThat(new isInstanceOf<Broker>()),
              argThat(new isInstanceOf<OffsetFetchRequest>())))
          .called(3);
    });

    test('it tries to refresh coordinator host 3 times on commitOffsets',
        () async {
      when(_session.getConsumerMetadata('testGroup')).thenReturn(
          new Future.value(new GroupCoordinatorResponse(0, _badCoordinator.id,
              _badCoordinator.host, _badCoordinator.port)));

      var group = new ConsumerGroup(_session, 'testGroup');
      var offsets = [new ConsumerOffset(_topicName, 0, 3, '')];

      try {
        await group.commitOffsets(offsets);
      } catch (e) {
        expect(e, new isInstanceOf<KafkaServerError>());
        expect(e.code, equals(16));
      }
      verify(_session.getConsumerMetadata('testGroup')).called(3);
    });

    test('it can reset offsets to earliest', () async {
      var offsetMaster = new OffsetMaster(_session);
      var earliestOffsets = await offsetMaster.fetchEarliest({
        _topicName: [0, 1, 2].toSet()
      });

      var group = new ConsumerGroup(_session, 'testGroup');
      await group.resetOffsetsToEarliest({
        _topicName: [0, 1, 2].toSet()
      });

      var offsets = await group.fetchOffsets({
        _topicName: [0, 1, 2].toSet()
      });
      expect(offsets, hasLength(3));

      for (var o in offsets) {
        var earliest =
            earliestOffsets.firstWhere((to) => to.partitionId == o.partitionId);
        expect(o.offset, equals(earliest.offset - 1));
      }
    });

    test('members can join consumer group', () async {
      var group = new ConsumerGroup(_session, 'newGroup');
      var membership = await group.join(15000, '', 'consumer', [
        new GroupProtocol.roundrobin(0, ['foo'].toSet())
      ]);
      expect(membership, new isInstanceOf<GroupMembership>());
      expect(membership.memberId, isNotEmpty);
      expect(membership.memberId, membership.leaderId);
      expect(membership.groupProtocol, 'roundrobin');
      expect(membership.assignment.partitionAssignment,
          containsPair('foo', [0, 1, 2]));
    });
  });
}
