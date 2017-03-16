import 'io.dart';
import 'errors.dart';
import 'common.dart';

class JoinGroupRequest implements KRequest<JoinGroupResponse> {
  @override
  final int apiKey = ApiKey.joinGroup;

  /// The name of consumer group to join.
  final String group;

  /// The coordinator considers the consumer dead if it receives no heartbeat
  /// after this timeout (in ms).
  final int sessionTimeout;

  /// The maximum time that the coordinator will wait for each member to rejoin
  /// when rebalancing the group.
  final int rebalanceTimeout;

  /// The assigned consumer id or an empty string for a new consumer.
  final String memberId;

  /// Unique name for class of protocols implemented by group.
  final String protocolType;

  /// List of protocols that the member supports.
  final List<GroupProtocol> groupProtocols;

  /// Creates new JoinGroupRequest.
  ///
  /// [sessionTimeout] (in msecs) defines window after which coordinator will
  /// consider group member dead if no heartbeats were received.
  ///
  /// When a member first joins the group, the [memberId] will be empty (""),
  /// but a rejoining member should use the same memberId from the previous generation.
  ///
  /// For details on [protocolType] see Kafka documentation. This library implements
  /// standard "consumer" embedded protocol described in Kafka docs.
  ///
  /// [groupProtocols] depends on `protocolType`. Each member joining member must
  /// provide list of protocols it supports. See Kafka docs for more details.
  JoinGroupRequest(this.group, this.sessionTimeout, this.rebalanceTimeout,
      this.memberId, this.protocolType, this.groupProtocols);

  @override
  ResponseDecoder<JoinGroupResponse> get decoder =>
      const _JoinGroupResponseDecoder();

  @override
  RequestEncoder<KRequest> get encoder => const _JoinGroupRequestEncoder();
}

abstract class GroupProtocol {
  String get protocolName;
  List<int> get protocolMetadata;
  Set<String> get topics;

  factory GroupProtocol.roundrobin(int version, Set<String> topics) {
    return new RoundRobinGroupProtocol(version, topics);
  }

  factory GroupProtocol.fromBytes(String name, List<int> data) {
    switch (name) {
      case 'roundrobin':
        return new RoundRobinGroupProtocol.fromBytes(data);
      default:
        throw new StateError('Unsupported group protocol "$name"');
    }
  }
}

class RoundRobinGroupProtocol implements GroupProtocol {
  @override
  List<int> get protocolMetadata {
    var builder = new KafkaBytesBuilder();
    builder
      ..addInt16(version)
      ..addStringArray(topics.toList())
      ..addBytes(null);
    return builder.takeBytes();
  }

  @override
  String get protocolName => 'roundrobin';

  final int version;

  final Set<String> topics;

  RoundRobinGroupProtocol(this.version, this.topics);

  factory RoundRobinGroupProtocol.fromBytes(List<int> data) {
    var reader = new KafkaBytesReader.fromBytes(data);
    var version = reader.readInt16();
    Set<String> topics = reader.readStringArray().toSet();
    reader.readBytes(); // user data (unused)

    return new RoundRobinGroupProtocol(version, topics);
  }
}

class JoinGroupResponse {
  final int error;
  final int generationId;
  final String groupProtocol;
  final String leaderId;
  final String memberId;
  final List<GroupMember> members;

  JoinGroupResponse(this.error, this.generationId, this.groupProtocol,
      this.leaderId, this.memberId, this.members) {
    if (error != Errors.NoError) throw new KafkaError.fromCode(error, this);
  }
}

class GroupMember {
  final String id;
  final List<int> metadata;

  GroupMember(this.id, this.metadata);
}

class _JoinGroupRequestEncoder implements RequestEncoder<JoinGroupRequest> {
  const _JoinGroupRequestEncoder();

  @override
  List<int> encode(JoinGroupRequest request, int version) {
    assert(version == 1,
        'Only v1 of JoinGroup request is supported by the client.');

    var builder = new KafkaBytesBuilder();
    builder.addString(request.group);
    builder.addInt32(request.sessionTimeout);
    builder.addInt32(request.rebalanceTimeout);
    builder.addString(request.memberId);
    builder.addString(request.protocolType);
    builder.addInt32(request.groupProtocols.length);
    request.groupProtocols.forEach((protocol) {
      builder.addString(protocol.protocolName);
      builder.addBytes(protocol.protocolMetadata);
    });
    return builder.takeBytes();
  }
}

class _JoinGroupResponseDecoder implements ResponseDecoder<JoinGroupResponse> {
  const _JoinGroupResponseDecoder();

  @override
  JoinGroupResponse decode(List<int> data) {
    var reader = new KafkaBytesReader.fromBytes(data);
    var error = reader.readInt16();
    var generationId = reader.readInt32();
    var groupProtocol = reader.readString();
    var leaderId = reader.readString();
    var memberId = reader.readString();
    List<GroupMember> members = reader
        .readObjectArray((_) => new GroupMember(_.readString(), _.readBytes()));
    return new JoinGroupResponse(
        error, generationId, groupProtocol, leaderId, memberId, members);
  }
}

class SyncGroupRequest implements KRequest<SyncGroupResponse> {
  @override
  final int apiKey = ApiKey.syncGroup;

  /// The name of consumer group.
  final String group;
  final int generationId;
  final String memberId;
  final List<GroupAssignment> groupAssignments;

  SyncGroupRequest(
      this.group, this.generationId, this.memberId, this.groupAssignments);

  @override
  ResponseDecoder<SyncGroupResponse> get decoder =>
      const _SyncGroupResponseDecoder();

  @override
  RequestEncoder<KRequest> get encoder => const _SyncGroupRequestEncoder();
}

class GroupAssignment {
  final String memberId;
  final MemberAssignment memberAssignment;

  GroupAssignment(this.memberId, this.memberAssignment);
}

class MemberAssignment {
  final int version;
  final Map<String, List<int>> partitions;
  final List<int> userData;

  MemberAssignment(this.version, this.partitions, this.userData);

  List<TopicPartition> _partitionsList;
  List<TopicPartition> get partitionsAsList {
    if (_partitionsList != null) return _partitionsList;
    var result = new List<TopicPartition>();
    for (var topic in partitions.keys) {
      result.addAll(partitions[topic].map((p) => new TopicPartition(topic, p)));
    }
    _partitionsList = result.toList(growable: false);
    return _partitionsList;
  }

  List<String> _topics;

  /// List of topic names in this member assignment.
  List<String> get topics {
    if (_topics != null) return _topics;
    _topics = partitions.keys.toList(growable: false);
    return _topics;
  }

  @override
  String toString() =>
      'MemberAssignment{version: $version, partitions: $partitions}';
}

class SyncGroupResponse {
  final int error;
  final MemberAssignment assignment;

  SyncGroupResponse(this.error, this.assignment) {
    if (error != Errors.NoError) throw new KafkaError.fromCode(error, this);
  }
}

class _SyncGroupRequestEncoder implements RequestEncoder<SyncGroupRequest> {
  const _SyncGroupRequestEncoder();

  @override
  List<int> encode(SyncGroupRequest request, int version) {
    assert(version == 0,
        'Only v0 of SyncGroup request is supported by the client.');

    var builder = new KafkaBytesBuilder();
    builder.addString(request.group);
    builder.addInt32(request.generationId);
    builder.addString(request.memberId);

    builder.addInt32(request.groupAssignments.length);
    request.groupAssignments.forEach((member) {
      builder.addString(member.memberId);
      builder.addBytes(_encodeMemberAssignment(member.memberAssignment));
    });
    return builder.takeBytes();
  }

  List<int> _encodeMemberAssignment(MemberAssignment assignment) {
    var builder = new KafkaBytesBuilder();
    builder.addInt16(assignment.version);
    builder.addInt32(assignment.partitions.length);
    for (var topic in assignment.partitions.keys) {
      builder.addString(topic);
      builder.addInt32Array(assignment.partitions[topic]);
    }
    builder.addBytes(assignment.userData);
    return builder.takeBytes();
  }
}

class _SyncGroupResponseDecoder implements ResponseDecoder<SyncGroupResponse> {
  const _SyncGroupResponseDecoder();

  @override
  SyncGroupResponse decode(List<int> data) {
    var reader = new KafkaBytesReader.fromBytes(data);
    var error = reader.readInt16();
    var assignmentData = reader.readBytes();
    MemberAssignment assignment;
    if (assignmentData.isNotEmpty) {
      var reader = new KafkaBytesReader.fromBytes(assignmentData);
      var version = reader.readInt16();
      var length = reader.readInt32();
      Map<String, List<int>> partitionAssignments = new Map();
      for (var i = 0; i < length; i++) {
        var topic = reader.readString();
        partitionAssignments[topic] = reader.readInt32Array();
      }
      var userData = reader.readBytes();
      assignment =
          new MemberAssignment(version, partitionAssignments, userData);
    }

    return new SyncGroupResponse(error, assignment);
  }
}

class LeaveGroupRequest implements KRequest<LeaveGroupResponse> {
  @override
  final int apiKey = ApiKey.leaveGroup;

  /// The name of consumer group.
  final String group;

  /// The ID of the member leaving the group.
  final String memberId;

  LeaveGroupRequest(this.group, this.memberId);

  @override
  ResponseDecoder<LeaveGroupResponse> get decoder =>
      const _LeaveGroupResponseDecoder();

  @override
  RequestEncoder<KRequest> get encoder => const _LeaveGroupRequestEncoder();
}

class LeaveGroupResponse {
  final int error;

  LeaveGroupResponse(this.error) {
    if (error != Errors.NoError) throw new KafkaError.fromCode(error, this);
  }
}

class _LeaveGroupRequestEncoder implements RequestEncoder<LeaveGroupRequest> {
  const _LeaveGroupRequestEncoder();

  @override
  List<int> encode(LeaveGroupRequest request, int version) {
    assert(version == 0,
        'Only v0 of LeaveGroup request is supported by the client.');
    var builder = new KafkaBytesBuilder();
    builder..addString(request.group)..addString(request.memberId);
    return builder.takeBytes();
  }
}

class _LeaveGroupResponseDecoder
    implements ResponseDecoder<LeaveGroupResponse> {
  const _LeaveGroupResponseDecoder();

  @override
  LeaveGroupResponse decode(List<int> data) {
    var reader = new KafkaBytesReader.fromBytes(data);
    var error = reader.readInt16();
    return new LeaveGroupResponse(error);
  }
}

class HeartbeatRequest implements KRequest<HeartbeatResponse> {
  @override
  final int apiKey = ApiKey.heartbeat;

  /// The name of consumer group.
  final String group;

  /// The ID of group generation.
  final int generationId;

  /// The ID of the member sending this heartbeat.
  final String memberId;

  HeartbeatRequest(this.group, this.generationId, this.memberId);

  @override
  ResponseDecoder<HeartbeatResponse> get decoder =>
      const _HeartbeatResponseDecoder();

  @override
  RequestEncoder<KRequest> get encoder => const _HeartbeatRequestEncoder();
}

class HeartbeatResponse {
  final int error;
  HeartbeatResponse(this.error) {
    if (error != Errors.NoError) throw new KafkaError.fromCode(error, this);
  }
}

class _HeartbeatRequestEncoder implements RequestEncoder<HeartbeatRequest> {
  const _HeartbeatRequestEncoder();

  @override
  List<int> encode(HeartbeatRequest request, int version) {
    assert(version == 0,
        'Only v0 of Heartbeat request is supported by the client.');
    var builder = new KafkaBytesBuilder();
    builder
      ..addString(request.group)
      ..addInt32(request.generationId)
      ..addString(request.memberId);
    return builder.takeBytes();
  }
}

class _HeartbeatResponseDecoder implements ResponseDecoder<HeartbeatResponse> {
  const _HeartbeatResponseDecoder();

  @override
  HeartbeatResponse decode(List<int> data) {
    var reader = new KafkaBytesReader.fromBytes(data);
    var error = reader.readInt16();
    return new HeartbeatResponse(error);
  }
}
