part of kafka.protocol;

class JoinGroupRequest extends KafkaRequest {
  /// API key for this [JoinGroupRequest].
  static const int apiKey = 11;

  /// API version for this [JoinGroupRequest].
  static const int apiVersion = 0;

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
  final Iterable<GroupProtocol> groupProtocols;

  /// Creates new JoinGroupRequest.
  ///
  /// [sessionTimeout] (in msecs) defines window after which coordinator will
  /// consider group member dead if no heartbeats were received
  ///
  /// When a member first joins the group, the [memberId] will be empty (i.e. ""),
  /// but a rejoining member should use the same memberId from the previous generation.
  ///
  /// For details on [protocolType] see Kafka documentation. This library implements
  /// standard "consumer" embedded protocol described in Kafka docs.
  ///
  /// [groupProtocols] depends on `protocolType`. Each member joining member must
  /// provide list of protocols it supports. See Kafka docs for more details.
  JoinGroupRequest(this.groupId, this.sessionTimeout, this.memberId,
      this.protocolType, this.groupProtocols)
      : super();

  @override
  createResponse(List<int> data) => new JoinGroupResponse.fromBytes(data);

  @override
  List<int> toBytes() {
    var builder = new KafkaBytesBuilder.withRequestHeader(
        apiKey, apiVersion, correlationId);

    builder.addString(groupId);
    builder.addInt32(sessionTimeout);
    builder.addString(memberId);
    builder.addString(protocolType);
    builder.addInt32(groupProtocols.length);
    groupProtocols.forEach((protocol) {
      builder.addString(protocol.protocolName);
      builder.addBytes(protocol.protocolMetadata);
    });

    var body = builder.takeBytes();
    builder.addBytes(body);

    return builder.takeBytes();
  }
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
      ..addInt16(version) // version
      ..addArray(topics, KafkaType.string)
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
    // ignore: STRONG_MODE_DOWN_CAST_COMPOSITE
    Set<String> topics = reader.readArray(KafkaType.string).toSet();
    reader.readBytes(); // user data (unused)

    return new RoundRobinGroupProtocol(version, topics);
  }
}

class JoinGroupResponse {
  final int errorCode;
  final int generationId;
  final String groupProtocol;
  final String leaderId;
  final String memberId;
  final Iterable<GroupMember> members;

  JoinGroupResponse(this.errorCode, this.generationId, this.groupProtocol,
      this.leaderId, this.memberId, this.members);

  factory JoinGroupResponse.fromBytes(List<int> data) {
    var reader = new KafkaBytesReader.fromBytes(data);
    var size = reader.readInt32();
    assert(size == data.length - 4);
    reader.readInt32(); // correlationId
    var errorCode = reader.readInt16();
    var generationId = reader.readInt32();
    var groupProtocol = reader.readString();
    var leaderId = reader.readString();
    var memberId = reader.readString();
    List<GroupMember> members = [];
    var length = reader.readInt32();
    for (var i = 0; i < length; i++) {
      members.add(new GroupMember(reader.readString(), reader.readBytes()));
    }
    var response = new JoinGroupResponse(
        errorCode, generationId, groupProtocol, leaderId, memberId, members);
    if (errorCode == KafkaServerError.NoError_) {
      return response;
    } else {
      throw new KafkaServerError.fromCode(errorCode, response);
    }
  }

  @override
  toString() => '''JoinGroupResponse(
  err: $errorCode,
  generationId: $generationId,
  groupProtocol: $groupProtocol,
  leaderId: $leaderId,
  memberId: $memberId,
  members: $members
)''';
}

class GroupMember {
  final String id;
  final List<int> metadata;

  GroupMember(this.id, this.metadata);
}

class SyncGroupRequest extends KafkaRequest {
  static const int apiKey = 14;
  static const int apiVersion = 0;

  final String groupId;
  final int generationId;
  final String memberId;
  final Iterable<GroupAssignment> groupAssignments;

  SyncGroupRequest(
      this.groupId, this.generationId, this.memberId, this.groupAssignments)
      : super();

  @override
  createResponse(List<int> data) {
    return new SyncGroupResponse.fromBytes(data);
  }

  @override
  List<int> toBytes() {
    var builder = new KafkaBytesBuilder.withRequestHeader(
        apiKey, apiVersion, correlationId);

    builder.addString(groupId);
    builder.addInt32(generationId);
    builder.addString(memberId);

    builder.addInt32(groupAssignments.length);
    groupAssignments.forEach((member) {
      builder.addString(member.memberId);
      builder.addBytes(member.memberAssignment.toBytes());
    });

    var body = builder.takeBytes();
    builder.addBytes(body);

    return builder.takeBytes();
  }
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

  List<int> toBytes() {
    var builder = new KafkaBytesBuilder();
    builder.addInt16(version);
    builder.addInt32(partitionAssignment.length);
    for (var topic in partitionAssignment.keys) {
      builder.addString(topic);
      builder.addArray(partitionAssignment[topic], KafkaType.int32);
    }
    builder.addBytes(userData);

    return builder.takeBytes();
  }

  factory MemberAssignment.fromBytes(List<int> data) {
    if (data.isEmpty) return null;

    var reader = new KafkaBytesReader.fromBytes(data);
    var version = reader.readInt16();

    var length = reader.readInt32();
    Map<String, List<int>> partitionAssignments = new Map();
    for (var i = 0; i < length; i++) {
      var topic = reader.readString();
      // ignore: STRONG_MODE_DOWN_CAST_COMPOSITE
      List<int> partitions = reader.readArray(KafkaType.int32);
      partitionAssignments[topic] = partitions;
    }
    var userData = reader.readBytes();
    return new MemberAssignment(version, partitionAssignments, userData);
  }
}

class SyncGroupResponse {
  final int errorCode;
  final MemberAssignment assignment;

  SyncGroupResponse(this.errorCode, this.assignment);

  factory SyncGroupResponse.fromBytes(List<int> data) {
    var reader = new KafkaBytesReader.fromBytes(data);
    var size = reader.readInt32();
    assert(size == data.length - 4);
    reader.readInt32(); // correlationId
    var errorCode = reader.readInt16();
    var assignment = reader.readBytes();

    var response = new SyncGroupResponse(
        errorCode, new MemberAssignment.fromBytes(assignment));
    if (errorCode == KafkaServerError.NoError_) {
      return response;
    } else {
      throw new KafkaServerError.fromCode(errorCode, response);
    }
  }
}

class HeartbeatRequest extends KafkaRequest {
  /// API key for Heartbeat requests.
  static const int apiKey = 12;

  /// API version for Heartbeat requests.
  static const int apiVersion = 0;

  final String groupId;
  final int generationId;
  final String memberId;

  HeartbeatRequest(this.groupId, this.generationId, this.memberId) : super();

  @override
  createResponse(List<int> data) => new HeartbeatResponse.fromBytes(data);

  @override
  List<int> toBytes() {
    var builder = new KafkaBytesBuilder.withRequestHeader(
        apiKey, apiVersion, correlationId);
    builder
      ..addString(groupId)
      ..addInt32(generationId)
      ..addString(memberId);

    var body = builder.takeBytes();
    builder.addBytes(body);

    return builder.takeBytes();
  }

  @override
  String toString() => 'HeartbeatRequest($groupId, $generationId, $memberId)';
}

class HeartbeatResponse {
  final int errorCode;

  HeartbeatResponse(this.errorCode);

  factory HeartbeatResponse.fromBytes(List<int> data) {
    var reader = new KafkaBytesReader.fromBytes(data);
    var size = reader.readInt32();
    assert(size == data.length - 4);
    reader.readInt32(); // correlationId
    var errorCode = reader.readInt16();

    var response = new HeartbeatResponse(errorCode);
    if (errorCode == KafkaServerError.NoError_) {
      return response;
    } else {
      throw new KafkaServerError.fromCode(errorCode, response);
    }
  }

  @override
  toString() => 'HeartbeatResponse(errorCode: $errorCode)';
}

class LeaveGroupRequest extends KafkaRequest {
  /// API key for Heartbeat requests.
  static const int apiKey = 13;

  /// API version for Heartbeat requests.
  static const int apiVersion = 0;

  final String groupId;
  final String memberId;

  LeaveGroupRequest(this.groupId, this.memberId);

  @override
  createResponse(List<int> data) {
    return new LeaveGroupResponse.fromBytes(data);
  }

  @override
  List<int> toBytes() {
    var builder = new KafkaBytesBuilder.withRequestHeader(
        apiKey, apiVersion, correlationId);
    builder..addString(groupId)..addString(memberId);

    var body = builder.takeBytes();
    builder.addBytes(body);

    return builder.takeBytes();
  }
}

class LeaveGroupResponse {
  final int errorCode;

  LeaveGroupResponse(this.errorCode);

  factory LeaveGroupResponse.fromBytes(List<int> data) {
    var reader = new KafkaBytesReader.fromBytes(data);
    var size = reader.readInt32();
    assert(size == data.length - 4);
    reader.readInt32(); // correlationId
    var errorCode = reader.readInt16();

    var response = new LeaveGroupResponse(errorCode);
    if (errorCode == KafkaServerError.NoError_) {
      return response;
    } else {
      throw new KafkaServerError.fromCode(errorCode, response);
    }
  }

  @override
  toString() => 'LeaveGroupResponse(errorCode: $errorCode)';
}
