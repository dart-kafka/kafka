part of kafka;

/// OffsetFetchRequest as defined in Kafka protocol.
///
/// This is a low-level API object. While it is posible to use this request
/// directly it is recommended to rely on high-level [Consumer] class which
/// encapsulates a lot of details about dealing with metadata and offsets.
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
  ///
  /// [host] must be current coordinator broker for [consumerGroup].
  OffsetFetchRequest(
      KafkaSession session, KafkaHost host, this.consumerGroup, this.topics)
      : super(session, host);

  Future<OffsetFetchResponse> send() async {
    return this.session.send(host, this);
  }

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
  _createResponse(List<int> data) {
    return new OffsetFetchResponse.fromData(data, correlationId);
  }
}

/// Result of [OffsetFetchResponse] as defined in Kafka protocol.
class OffsetFetchResponse {
  final Map<String, List<ConsumerOffset>> offsets = new Map();

  OffsetFetchResponse.fromOffsets(Map<String, List<ConsumerOffset>> offsets) {
    this.offsets.addAll(offsets);
  }

  OffsetFetchResponse.fromData(List<int> data, int correlationId) {
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
      offsets[topicName] = new List();
      var partitionCount = reader.readInt32();
      while (partitionCount > 0) {
        var id = reader.readInt32();
        var offset = reader.readInt64();
        var metadata = reader.readString();
        var errorCode = reader.readInt16();
        offsets[topicName]
            .add(new ConsumerOffset(id, offset, metadata, errorCode));
        partitionCount--;
      }
      count--;
    }
  }
}
