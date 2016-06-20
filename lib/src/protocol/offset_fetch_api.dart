part of kafka.protocol;

/// Kafka OffsetFetchRequest.
class OffsetFetchRequest extends KafkaRequest {
  /// API key of [OffsetFetchRequest]
  final int apiKey = 9;

  /// API version of [OffsetFetchRequest].
  final int apiVersion = 1;

  /// Name of consumer group.
  final String consumerGroup;

  /// Map of topic names and partition IDs.
  final Map<String, Set<int>> topics;

  /// Creates new instance of [OffsetFetchRequest].
  OffsetFetchRequest(this.consumerGroup, this.topics) : super();

  @override
  List<int> toBytes() {
    var builder = new KafkaBytesBuilder.withRequestHeader(
        apiKey, apiVersion, correlationId);

    builder.addString(consumerGroup);
    builder.addInt32(topics.length);
    topics.forEach((topicName, partitions) {
      builder.addString(topicName);
      builder.addArray(partitions, KafkaType.int32);
    });

    var body = builder.takeBytes();
    builder.addBytes(body);

    return builder.takeBytes();
  }

  @override
  createResponse(List<int> data) {
    return new OffsetFetchResponse.fromData(data);
  }
}

/// Kafka OffsetFetchResponse.
class OffsetFetchResponse {
  final List<ConsumerOffset> offsets;

  OffsetFetchResponse._(this.offsets);

  factory OffsetFetchResponse.fromOffsets(List<ConsumerOffset> offsets) {
    return new OffsetFetchResponse._(new List.from(offsets));
  }

  factory OffsetFetchResponse.fromData(List<int> data) {
    List<ConsumerOffset> offsets = [];
    var reader = new KafkaBytesReader.fromBytes(data);
    var size = reader.readInt32();
    assert(size == data.length - 4);

    reader.readInt32(); // correlationId
    var count = reader.readInt32();
    while (count > 0) {
      var topicName = reader.readString();
      var partitionCount = reader.readInt32();
      while (partitionCount > 0) {
        var id = reader.readInt32();
        var offset = reader.readInt64();
        var metadata = reader.readString();
        var errorCode = reader.readInt16();
        offsets.add(
            new ConsumerOffset(topicName, id, offset, metadata, errorCode));
        partitionCount--;
      }
      count--;
    }

    return new OffsetFetchResponse._(offsets);
  }
}
