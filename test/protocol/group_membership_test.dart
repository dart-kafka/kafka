library kafka.test.api.group_membership;

import 'package:kafka/kafka.dart';
import 'package:kafka/protocol.dart';
import 'package:test/test.dart';
import '../setup.dart';

void main() {
  group('GroupMembershipApi:', () {
    String groupId;
    String _topicName = 'dartKafkaTest';
    Broker _broker;
    KafkaSession _session;

    setUp(() async {
      var now = new DateTime.now().millisecondsSinceEpoch.toString();
      groupId = 'test-group-' + now;
      var host = await getDefaultHost();
      _session = new KafkaSession([new ContactPoint(host, 9092)]);
      var meta = await _session.getConsumerMetadata(groupId);
      _broker = meta.coordinator;
    });

    tearDown(() async {
      await _session.close();
    });

    test('we can join and leave a consumer group', () async {
      var protocols = [
        new GroupProtocol.roundrobin(0, [_topicName].toSet())
      ];
      var groupRequest =
          new JoinGroupRequest(groupId, 15000, '', 'consumer', protocols);
      JoinGroupResponse joinResponse =
          await _session.send(_broker, groupRequest);
      expect(joinResponse, new isInstanceOf<JoinGroupResponse>());
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
      var syncRequest = new SyncGroupRequest(groupId, joinResponse.generationId,
          joinResponse.memberId, assignments);
      SyncGroupResponse syncResponse =
          await _session.send(_broker, syncRequest);
      expect(syncResponse.errorCode, KafkaServerError.NoError_);
      expect(syncResponse.assignment.partitionAssignment, topics);

      var leaveRequest = new LeaveGroupRequest(groupId, joinResponse.memberId);
      LeaveGroupResponse leaveResponse =
          await _session.send(_broker, leaveRequest);
      expect(leaveResponse.errorCode, KafkaServerError.NoError_);
    });
  });
}
