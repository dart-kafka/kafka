part of kafka;

/// OffsetCommitRequest as defined in Kafka protocol spec.
///
/// This is a low-level API object. While it is posible to use this request
/// directly it is recommended to rely on high-level [KafkaConsumer] class which
/// encapsulates a lot of details about dealing with metadata and offsets.
class OffsetCommitRequest extends KafkaRequest {
  /// API key of [OffsetCommitRequest]
  final int apiKey = 8;

  /// API version of [OffsetCommitRequest]
  final int apiVersion = 1;

  final String consumerGroup;
  final int consumerGroupGenerationId;
  final String consumerId;

  final Map<String, List<_PartitionOffsetCommitInfo>> _topics = new Map();

  /// Creates new instance of [OffsetFetchRequest].
  ///
  /// [host] must be current coordinator broker for [consumerGroup].
  OffsetCommitRequest(KafkaClient client, KafkaHost host, this.consumerGroup,
      this.consumerGroupGenerationId, this.consumerId)
      : super(client, host);

  /// Adds offset to be committed for particular topic-partition.
  void addTopicPartitionOffset(String topicName, int partitionId, int offset,
      int timestamp, String metadata) {
    if (!_topics.containsKey(topicName)) {
      _topics[topicName] = new List();
    }
    _topics[topicName].add(new _PartitionOffsetCommitInfo(
        partitionId, offset, timestamp, metadata));
  }

  Future<OffsetCommitResponse> send() async {
    var data = await this.client.send(host, this);
    return new OffsetCommitResponse.fromData(data, correlationId);
  }

  @override
  List<int> toBytes() {
    var builder = new KafkaBytesBuilder.withRequestHeader(
        apiKey, apiVersion, correlationId);

    builder.addString(consumerGroup);
    builder.addInt32(consumerGroupGenerationId);
    builder.addString(consumerId);
    builder.addInt32(_topics.length);
    _topics.forEach((topicName, partitions) {
      builder.addString(topicName);
      builder.addInt32(partitions.length);
      partitions.forEach((p) {
        builder.addInt32(p.partitionId);
        builder.addInt64(p.offset);
        builder.addInt64(p.timestamp);
        builder.addString(p.metadata);
      });
    });

    var body = builder.takeBytes();
    builder.addBytes(body);

    return builder.takeBytes();
  }
}

class _PartitionOffsetCommitInfo {
  final int partitionId;
  final int offset;
  final int timestamp;
  final String metadata;
  _PartitionOffsetCommitInfo(
      this.partitionId, this.offset, this.timestamp, this.metadata);
}

class OffsetCommitResponse {
  final Map<String, List<PartitionOffsetCommitResult>> topics = new Map();

  OffsetCommitResponse.fromData(List<int> data, int correlationId) {
    var reader = new KafkaBytesReader.fromBytes(data);
    var size = reader.readInt32();
    assert(size == data.length - 4);

    var receivedCorrelationId = reader.readInt32();
    if (receivedCorrelationId != correlationId) {
      throw new CorrelationIdMismatchError(
          'Original value: $correlationId, received: $receivedCorrelationId');
    }

    var count = reader.readInt32();
    while (count > 0) {
      var topicName = reader.readString();
      topics[topicName] = new List();
      var partitionCount = reader.readInt32();
      while (partitionCount > 0) {
        topics[topicName].add(PartitionOffsetCommitResult.readFrom(reader));
        partitionCount--;
      }
      count--;
    }
  }
}

class PartitionOffsetCommitResult {
  final int partitionId;
  final int errorCode;

  PartitionOffsetCommitResult(this.partitionId, this.errorCode);

  static PartitionOffsetCommitResult readFrom(KafkaBytesReader reader) {
    var id = reader.readInt32();
    var errorCode = reader.readInt16();
    return new PartitionOffsetCommitResult(id, errorCode);
  }
}
