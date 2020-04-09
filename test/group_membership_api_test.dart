import 'package:kafka/kafka.dart';
import 'package:test/test.dart';

void main() {
  group('GroupMembershipApi:', () {
    String group;
    String _topic = 'dartKafkaTest';
    Broker _broker;
    Session _session = new Session(['127.0.0.1:9092']);

    setUp(() async {
      var now = new DateTime.now().millisecondsSinceEpoch.toString();
      group = 'test-group-' + now;
      _broker = await _session.metadata.fetchGroupCoordinator(group);
    });

    tearDownAll(() async {
      await _session.close();
    });

    test('we can join and leave a consumer group', () async {
      var protocols = [
        new GroupProtocol.roundrobin(0, [_topic].toSet())
      ];
      var joinRequest =
          new JoinGroupRequest(group, 15000, 1000, '', 'consumer', protocols);
      JoinGroupResponse joinResponse =
          await _session.send(joinRequest, _broker.host, _broker.port);
      expect(joinResponse, isA<JoinGroupResponse>());
      expect(joinResponse.error, 0);
      expect(joinResponse.generationId, greaterThanOrEqualTo(1));
      expect(joinResponse.groupProtocol, 'roundrobin');
      expect(joinResponse.leaderId, joinResponse.memberId);
      expect(joinResponse.members, hasLength(1));

      var topics = {
        _topic: [0, 1, 2]
      };
      var memberAssignment = new MemberAssignment(0, topics, null);
      var assignments = [
        new GroupAssignment(joinResponse.memberId, memberAssignment)
      ];
      var syncRequest = new SyncGroupRequest(
          group, joinResponse.generationId, joinResponse.memberId, assignments);
      SyncGroupResponse syncResponse =
          await _session.send(syncRequest, _broker.host, _broker.port);
      expect(syncResponse.error, Errors.NoError);
      expect(syncResponse.assignment.partitions, topics);

      var heartbeatRequest = new HeartbeatRequest(
          group, joinResponse.generationId, joinResponse.memberId);
      HeartbeatResponse heartbeatResponse =
          await _session.send(heartbeatRequest, _broker.host, _broker.port);
      expect(heartbeatResponse.error, Errors.NoError);

      var leaveRequest = new LeaveGroupRequest(group, joinResponse.memberId);
      LeaveGroupResponse leaveResponse =
          await _session.send(leaveRequest, _broker.host, _broker.port);
      expect(leaveResponse.error, Errors.NoError);
    });
  });
}
