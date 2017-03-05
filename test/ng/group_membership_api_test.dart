import 'package:kafka/ng.dart';
import 'package:test/test.dart';

void main() {
  group('GroupMembershipApi:', () {
    String groupId;
    String _topicName = 'dartKafkaTest';
    Broker _broker;
    Session _session = new Session([new ContactPoint('127.0.0.1:9092')]);
    Metadata _metadata = new Metadata(_session);

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
          new JoinGroupRequest(groupId, 15000, '', 'consumer', protocols);
      JoinGroupResponse joinResponse =
          await _session.send(joinRequest, _broker.host, _broker.port);
      expect(joinResponse, new isInstanceOf<JoinGroupResponse>());
      expect(joinResponse.error, 0);
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
      var syncRequest = new SyncGroupRequest(groupId, joinResponse.generationId,
          joinResponse.memberId, assignments);
      SyncGroupResponse syncResponse =
          await _session.send(syncRequest, _broker.host, _broker.port);
      expect(syncResponse.error, Errors.NoError);
      expect(syncResponse.assignment.partitionAssignment, topics);

      var heartbeatRequest = new HeartbeatRequest(
          groupId, joinResponse.generationId, joinResponse.memberId);
      HeartbeatResponse heartbeatResponse =
          await _session.send(heartbeatRequest, _broker.host, _broker.port);
      expect(heartbeatResponse.error, Errors.NoError);

      var leaveRequest = new LeaveGroupRequest(groupId, joinResponse.memberId);
      LeaveGroupResponse leaveResponse =
          await _session.send(leaveRequest, _broker.host, _broker.port);
      expect(leaveResponse.error, Errors.NoError);
    });
  });
}
