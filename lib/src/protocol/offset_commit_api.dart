part of kafka.protocol;

/// Kafka OffsetCommitRequest.
class OffsetCommitRequest extends KafkaRequest {
  /// API key of [OffsetCommitRequest].
  final int apiKey = 8;

  /// API version of [OffsetCommitRequest].
  final int apiVersion = 1;

  /// Name of the consumer group.
  final String consumerGroup;

  /// Generation ID of the consumer group.
  final int consumerGroupGenerationId;

  /// ID of the consumer.
  final String consumerId;

  /// Time period in msec to retain the offset.
  final int retentionTime;

  /// List of consumer offsets to be committed.
  final List<ConsumerOffset> offsets;

  /// Creates new instance of [OffsetCommitRequest].
  ///
  /// [host] must be current coordinator broker for [consumerGroup].
  OffsetCommitRequest(this.consumerGroup, this.offsets,
      this.consumerGroupGenerationId, this.consumerId, this.retentionTime)
      : super();

  @override
  List<int> toBytes() {
    var builder = new KafkaBytesBuilder.withRequestHeader(
        apiKey, apiVersion, correlationId);

    // TODO: replace groupBy with ListMultimap
    // ignore: STRONG_MODE_DOWN_CAST_COMPOSITE
    Map<String, List<ConsumerOffset>> groupedByTopic = groupBy(
        offsets, (o) => o.topicName); // ignore: STRONG_MODE_DOWN_CAST_COMPOSITE
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

/// Kafka OffsetCommitResponse.
class OffsetCommitResponse {
  final List<OffsetCommitResult> offsets;

  OffsetCommitResponse._(this.offsets);

  factory OffsetCommitResponse.fromData(List<int> data) {
    List<OffsetCommitResult> offsets = [];
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

/// Data structure representing result of commiting of consumer offset.
class OffsetCommitResult {
  final String topicName;
  final int partitionId;
  final int errorCode;

  OffsetCommitResult(this.topicName, this.partitionId, this.errorCode);
}
