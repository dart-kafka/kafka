import 'package:kafka/ng.dart';
import 'package:test/test.dart';

void main() {
  group('GroupMembershipApi:', () {
    String groupId;
    String _topicName = 'dartKafkaTest';
    Broker _broker;
    KSession _session = new KSession([new ContactPoint('127.0.0.1:9092')]);
    KMetadata _metadata = new KMetadata(_session);

    setUp(() async {
      var now = new DateTime.now().millisecondsSinceEpoch.toString();
      groupId = 'test-group-' + now;
      _broker = await _metadata.fetchGroupCoordinator(groupId);
    });

    tearDownAll(() async {
      await _session.close();
    });

    test('we can join and leave a consumer group', () async {
      var protocols = [
        new GroupProtocol.roundrobin(0, [_topicName].toSet())
      ];
      var joinRequest =
          new JoinGroupRequestV0(groupId, 15000, '', 'consumer', protocols);
      JoinGroupResponseV0 joinResponse =
          await _session.send(joinRequest, _broker.host, _broker.port);
      expect(joinResponse, new isInstanceOf<JoinGroupResponseV0>());
      expect(joinResponse.errorCode, 0);
      expect(joinResponse.generationId, greaterThanOrEqualTo(1));
      expect(joinResponse.groupProtocol, 'roundrobin');
      expect(joinResponse.leaderId, joinResponse.memberId);
      expect(joinResponse.members, hasLength(1));

      var topics = {
        _topicName: [0, 1, 2]
      };
      var memberAssignment = new MemberAssignment(0, topics, null);
      var assignments = [
        new GroupAssignment(joinResponse.memberId, memberAssignment)
      ];
      var syncRequest = new SyncGroupRequestV0(groupId,
          joinResponse.generationId, joinResponse.memberId, assignments);
      SyncGroupResponseV0 syncResponse =
          await _session.send(syncRequest, _broker.host, _broker.port);
      expect(syncResponse.errorCode, KafkaServerError.NoError_);
      expect(syncResponse.assignment.partitionAssignment, topics);

      var heartbeatRequest = new HeartbeatRequestV0(
          groupId, joinResponse.generationId, joinResponse.memberId);
      HeartbeatResponseV0 heartbeatResponse =
          await _session.send(heartbeatRequest, _broker.host, _broker.port);
      expect(heartbeatResponse.errorCode, KafkaServerError.NoError_);

      var leaveRequest =
          new LeaveGroupRequestV0(groupId, joinResponse.memberId);
      LeaveGroupResponseV0 leaveResponse =
          await _session.send(leaveRequest, _broker.host, _broker.port);
      expect(leaveResponse.errorCode, KafkaServerError.NoError_);
    });
  });
}
