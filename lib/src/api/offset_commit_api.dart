part of kafka.protocol;

/// OffsetCommitRequest as defined in Kafka protocol spec.
///
/// This is a low-level API object. While it is posible to use this request
/// directly it is recommended to rely on high-level [Consumer] class which
/// encapsulates a lot of details about dealing with metadata and offsets.
class OffsetCommitRequest extends KafkaRequest {
  /// API key of [OffsetCommitRequest]
  final int apiKey = 8;

  /// API version of [OffsetCommitRequest]
  final int apiVersion = 1;

  final String consumerGroup;
  final int consumerGroupGenerationId;
  final String consumerId;

  final List<ConsumerOffset> offsets;

  /// Creates new instance of [OffsetCommitRequest].
  ///
  /// [host] must be current coordinator broker for [consumerGroup].
  OffsetCommitRequest(this.consumerGroup, this.offsets,
      this.consumerGroupGenerationId, this.consumerId)
      : super();

  @override
  List<int> toBytes() {
    var builder = new KafkaBytesBuilder.withRequestHeader(
        apiKey, apiVersion, correlationId);

    Map<String, List<ConsumerOffset>> groupedByTopic =
        groupBy(offsets, (o) => o.topicName);
    var timestamp = new DateTime.now().millisecondsSinceEpoch;
    builder.addString(consumerGroup);
    builder.addInt32(consumerGroupGenerationId);
    builder.addString(consumerId);
    builder.addInt32(groupedByTopic.length);
    groupedByTopic.forEach((topicName, partitionOffsets) {
      builder.addString(topicName);
      builder.addInt32(partitionOffsets.length);
      partitionOffsets.forEach((p) {
        builder.addInt32(p.partitionId);
        builder.addInt64(p.offset);
        builder.addInt64(timestamp);
        builder.addString(p.metadata);
      });
    });

    var body = builder.takeBytes();
    builder.addBytes(body);

    return builder.takeBytes();
  }

  @override
  createResponse(List<int> data) {
    return new OffsetCommitResponse.fromData(data);
  }
}

class OffsetCommitResponse {
  final List<OffsetCommitResult> offsets;

  OffsetCommitResponse._(this.offsets);

  factory OffsetCommitResponse.fromData(List<int> data) {
    var offsets = [];
    var reader = new KafkaBytesReader.fromBytes(data);
    var size = reader.readInt32();
    assert(size == data.length - 4);

    reader.readInt32(); // correlationId
    var count = reader.readInt32();
    while (count > 0) {
      var topicName = reader.readString();
      var partitionCount = reader.readInt32();
      while (partitionCount > 0) {
        var partitionId = reader.readInt32();
        var errorCode = reader.readInt16();
        offsets.add(new OffsetCommitResult(topicName, partitionId, errorCode));
        partitionCount--;
      }
      count--;
    }

    return new OffsetCommitResponse._(offsets);
  }
}

class OffsetCommitResult {
  final String topicName;
  final int partitionId;
  final int errorCode;

  OffsetCommitResult(this.topicName, this.partitionId, this.errorCode);
}
