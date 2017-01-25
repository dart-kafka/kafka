import 'io.dart';
import 'errors.dart';

class JoinGroupRequestV0 implements KRequest<JoinGroupResponseV0> {
  @override
  final int apiKey = 11;

  @override
  final int apiVersion = 0;

  /// The group ID.
  final String groupId;

  /// The coordinator considers the consumer dead if it receives no heartbeat
  /// after this timeout (in ms).
  final int sessionTimeout;

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
  JoinGroupRequestV0(this.groupId, this.sessionTimeout, this.memberId,
      this.protocolType, this.groupProtocols);

  @override
  ResponseDecoder<JoinGroupResponseV0> get decoder =>
      new _JoinGroupResponseV0Decoder();

  @override
  RequestEncoder<KRequest> get encoder => new _JoinGroupRequestV0Encoder();
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

class JoinGroupResponseV0 {
  final int errorCode;
  final int generationId;
  final String groupProtocol;
  final String leaderId;
  final String memberId;
  final Iterable<GroupMember> members;

  JoinGroupResponseV0(this.errorCode, this.generationId, this.groupProtocol,
      this.leaderId, this.memberId, this.members) {
    if (errorCode != KafkaServerError.NoError_) {
      throw new KafkaServerError.fromCode(errorCode, this);
    }
  }
}

class GroupMember {
  final String id;
  final List<int> metadata;

  GroupMember(this.id, this.metadata);
}

class _JoinGroupRequestV0Encoder implements RequestEncoder<JoinGroupRequestV0> {
  @override
  List<int> encode(JoinGroupRequestV0 request) {
    var builder = new KafkaBytesBuilder();

    builder.addString(request.groupId);
    builder.addInt32(request.sessionTimeout);
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

class _JoinGroupResponseV0Decoder
    implements ResponseDecoder<JoinGroupResponseV0> {
  @override
  JoinGroupResponseV0 decode(List<int> data) {
    var reader = new KafkaBytesReader.fromBytes(data);
    var errorCode = reader.readInt16();
    var generationId = reader.readInt32();
    var groupProtocol = reader.readString();
    var leaderId = reader.readString();
    var memberId = reader.readString();
    List<GroupMember> members = reader
        .readObjectArray((_) => new GroupMember(_.readString(), _.readBytes()));
    return new JoinGroupResponseV0(
        errorCode, generationId, groupProtocol, leaderId, memberId, members);
  }
}

class SyncGroupRequestV0 implements KRequest<SyncGroupResponseV0> {
  @override
  final int apiKey = 14;

  @override
  final int apiVersion = 0;

  final String groupId;
  final int generationId;
  final String memberId;
  final List<GroupAssignment> groupAssignments;

  SyncGroupRequestV0(
      this.groupId, this.generationId, this.memberId, this.groupAssignments);

  @override
  ResponseDecoder<SyncGroupResponseV0> get decoder =>
      new _SyncGroupResponseV0Decoder();

  @override
  RequestEncoder<KRequest> get encoder => new _SyncGroupRequestV0Encoder();
}

class GroupAssignment {
  final String memberId;
  final MemberAssignment memberAssignment;

  GroupAssignment(this.memberId, this.memberAssignment);
}

class MemberAssignment {
  final int version;
  final Map<String, List<int>> partitionAssignment;
  final List<int> userData;

  MemberAssignment(this.version, this.partitionAssignment, this.userData);
}

class SyncGroupResponseV0 {
  final int errorCode;
  final MemberAssignment assignment;

  SyncGroupResponseV0(this.errorCode, this.assignment) {
    if (errorCode != KafkaServerError.NoError_) {
      throw new KafkaServerError.fromCode(errorCode, this);
    }
  }
}

class _SyncGroupRequestV0Encoder implements RequestEncoder<SyncGroupRequestV0> {
  @override
  List<int> encode(SyncGroupRequestV0 request) {
    var builder = new KafkaBytesBuilder();

    builder.addString(request.groupId);
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
    builder.addInt32(assignment.partitionAssignment.length);
    for (var topic in assignment.partitionAssignment.keys) {
      builder.addString(topic);
      builder.addInt32Array(assignment.partitionAssignment[topic]);
    }
    builder.addBytes(assignment.userData);
    return builder.takeBytes();
  }
}

class _SyncGroupResponseV0Decoder
    implements ResponseDecoder<SyncGroupResponseV0> {
  @override
  SyncGroupResponseV0 decode(List<int> data) {
    var reader = new KafkaBytesReader.fromBytes(data);
    var errorCode = reader.readInt16();
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

    return new SyncGroupResponseV0(errorCode, assignment);
  }
}

class LeaveGroupRequestV0 implements KRequest<LeaveGroupResponseV0> {
  @override
  final int apiKey = 13;

  @override
  final int apiVersion = 0;

  final String groupId;
  final String memberId;

  LeaveGroupRequestV0(this.groupId, this.memberId);

  @override
  ResponseDecoder<LeaveGroupResponseV0> get decoder =>
      new _LeaveGroupResponseV0Decoder();

  @override
  RequestEncoder<KRequest> get encoder => new _LeaveGroupRequestV0Encoder();
}

class LeaveGroupResponseV0 {
  final int errorCode;
  LeaveGroupResponseV0(this.errorCode) {
    if (errorCode != KafkaServerError.NoError_) {
      throw new KafkaServerError.fromCode(errorCode, this);
    }
  }
}

class _LeaveGroupRequestV0Encoder
    implements RequestEncoder<LeaveGroupRequestV0> {
  @override
  List<int> encode(LeaveGroupRequestV0 request) {
    var builder = new KafkaBytesBuilder();
    builder..addString(request.groupId)..addString(request.memberId);
    return builder.takeBytes();
  }
}

class _LeaveGroupResponseV0Decoder
    implements ResponseDecoder<LeaveGroupResponseV0> {
  @override
  LeaveGroupResponseV0 decode(List<int> data) {
    var reader = new KafkaBytesReader.fromBytes(data);
    var errorCode = reader.readInt16();
    return new LeaveGroupResponseV0(errorCode);
  }
}

class HeartbeatRequestV0 implements KRequest<HeartbeatResponseV0> {
  @override
  final int apiKey = 12;

  @override
  final int apiVersion = 0;

  final String groupId;
  final int generationId;
  final String memberId;

  HeartbeatRequestV0(this.groupId, this.generationId, this.memberId);

  @override
  ResponseDecoder<HeartbeatResponseV0> get decoder =>
      new _HeartbeatResponseV0Decoder();

  @override
  RequestEncoder<KRequest> get encoder => new _HeartbeatRequestV0Encoder();
}

class HeartbeatResponseV0 {
  final int errorCode;
  HeartbeatResponseV0(this.errorCode) {
    if (errorCode != KafkaServerError.NoError_) {
      throw new KafkaServerError.fromCode(errorCode, this);
    }
  }
}

class _HeartbeatRequestV0Encoder implements RequestEncoder<HeartbeatRequestV0> {
  @override
  List<int> encode(HeartbeatRequestV0 request) {
    var builder = new KafkaBytesBuilder();
    builder
      ..addString(request.groupId)
      ..addInt32(request.generationId)
      ..addString(request.memberId);
    return builder.takeBytes();
  }
}

class _HeartbeatResponseV0Decoder
    implements ResponseDecoder<HeartbeatResponseV0> {
  @override
  HeartbeatResponseV0 decode(List<int> data) {
    var reader = new KafkaBytesReader.fromBytes(data);
    var errorCode = reader.readInt16();
    return new HeartbeatResponseV0(errorCode);
  }
}
