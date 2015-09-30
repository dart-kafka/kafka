part of kafka;

/// OffsetRequest as defined in Kafka protocol spec.
///
/// This is a low-level API object and requires good knowledge of Kafka protocol.
class OffsetRequest extends KafkaRequest {
  /// API key of [OffsetRequest].
  final int apiKey = 2;

  /// API version of [OffsetRequest].
  final int apiVersion = 0;

  /// Unique ID assigned to the [host] within Kafka cluster.
  final int replicaId;

  Map<String, List<_PartitionOffsetRequestInfo>> _topics = new Map();

  /// Creates new instance of OffsetRequest.
  ///
  /// The [replicaId] argument indicates unique ID assigned to the [host] within
  /// Kafka cluster. One can obtain this information via [MetadataRequest].
  OffsetRequest(KafkaClient client, KafkaHost host, this.replicaId)
      : super(client, host);

  /// Adds topic and partition to this requests.
  ///
  /// [time] is used to ask for all messages before a certain time (ms).
  /// There are two special values:
  /// * Specify -1 to receive the latest offset (that is the offset of the next coming message).
  /// * Specify -2 to receive the earliest available offset.
  ///
  /// [maxNumberOfOffsets] indicates max number of offsets to return.
  void addTopicPartition(
      String topicName, int partitionId, int time, int maxNumberOfOffsets) {
    if (_topics.containsKey(topicName) == false) {
      _topics[topicName] = new List();
    }

    _topics[topicName].add(
        new _PartitionOffsetRequestInfo(partitionId, time, maxNumberOfOffsets));
  }

  /// Sends this request to the server specified in the [host] property.
  Future<OffsetResponse> send() async {
    var data = await client.send(host, this);
    return new OffsetResponse.fromData(data, correlationId);
  }

  /// Converts this request into a byte array.
  @override
  List<int> toBytes() {
    var builder = new KafkaBytesBuilder.withRequestHeader(
        apiKey, apiVersion, correlationId);
    builder.addInt32(replicaId);

    builder.addInt32(_topics.length);
    _topics.forEach((topicName, partitions) {
      builder.addString(topicName);
      builder.addInt32(partitions.length);
      partitions.forEach((p) {
        builder.addInt32(p.partitionId);
        builder.addInt64(p.time);
        builder.addInt32(p.maxNumberOfOffsets);
      });
    });

    var body = builder.takeBytes();
    builder.addBytes(body);

    return builder.takeBytes();
  }
}

/// Value object holding information about partition offsets to be fetched
/// by [OffsetRequest].
class _PartitionOffsetRequestInfo {
  /// The ID of this partition.
  final int partitionId;

  /// Used to ask for all messages before a certain time (ms).
  ///
  /// There are two special values:
  /// * Specify -1 to receive the latest offset (that is the offset of the next coming message).
  /// * Specify -2 to receive the earliest available offset.
  final int time;

  /// How many offsets to return.
  final int maxNumberOfOffsets;
  _PartitionOffsetRequestInfo(
      this.partitionId, this.time, this.maxNumberOfOffsets);
}

/// Result of [OffsetRequest] as defined in Kafka protocol spec.
///
/// This is a low-level API object and requires extensive knowledge of Kafka
/// protocol.
class OffsetResponse {
  /// Map of topics and list of partitions with offset details.
  final Map<String, List<PartitionOffsetsInfo>> topics = new Map();

  /// Creates OffsetResponse from the provided byte array.
  OffsetResponse.fromData(List<int> data, int correlationId) {
    var reader = new KafkaBytesReader.fromBytes(data);
    var size = reader.readInt32();
    assert(size == data.length - 4);

    var receivedCorrelationId = reader.readInt32();
    assert(receivedCorrelationId == correlationId); // TODO: throw exception?

    var count = reader.readInt32();
    while (count > 0) {
      var topicName = reader.readString();
      topics[topicName] = new List();
      var partitionCount = reader.readInt32();
      while (partitionCount > 0) {
        topics[topicName].add(new PartitionOffsetsInfo.readFrom(reader));
        partitionCount--;
      }
      count--;
    }
  }
}

/// Holds information about partition offsets returned from the server in
/// [OffsetResponse].
class PartitionOffsetsInfo {
  final int partitionId;
  final int errorCode;
  final List<int> offsets;

  PartitionOffsetsInfo.readFrom(KafkaBytesReader reader)
      : partitionId = reader.readInt32(),
        errorCode = reader.readInt16(),
        offsets = reader.readArray(KafkaType.int64);
}
