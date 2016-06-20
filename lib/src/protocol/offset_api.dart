part of kafka.protocol;

/// Kafka OffsetRequest.
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
  OffsetRequest(this.replicaId) : super();

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

  /// Converts this request into a binary representation according to Kafka
  /// protocol.
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

  @override
  createResponse(List<int> data) {
    return new OffsetResponse.fromBytes(data);
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

/// Kafka OffsetResponse.
class OffsetResponse {
  /// Map of topics and list of partitions with offset details.
  final List<TopicOffsets> offsets;

  OffsetResponse._(this.offsets);

  /// Creates OffsetResponse from the provided binary data.
  factory OffsetResponse.fromBytes(List<int> data) {
    var reader = new KafkaBytesReader.fromBytes(data);
    var size = reader.readInt32();
    assert(size == data.length - 4);

    reader.readInt32(); // correlationId
    var count = reader.readInt32();
    var offsets = new List<TopicOffsets>();
    while (count > 0) {
      var topicName = reader.readString();
      var partitionCount = reader.readInt32();
      while (partitionCount > 0) {
        var partitionId = reader.readInt32();
        var errorCode = reader.readInt16();
        var partitionOffsets = reader.readArray(KafkaType.int64);
        offsets.add(new TopicOffsets._(topicName, partitionId, errorCode,
            partitionOffsets)); // ignore: STRONG_MODE_DOWN_CAST_COMPOSITE
        partitionCount--;
      }
      count--;
    }

    return new OffsetResponse._(offsets);
  }
}

/// Data structure representing offsets of particular topic-partition returned
/// by [OffsetRequest].
class TopicOffsets {
  final String topicName;
  final int partitionId;
  final int errorCode;
  final List<int> offsets;

  TopicOffsets._(
      this.topicName, this.partitionId, this.errorCode, this.offsets);
}
