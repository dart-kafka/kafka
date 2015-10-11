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

  final Map<String, List<ConsumerOffset>> offsets;

  /// Creates new instance of [OffsetCommitRequest].
  ///
  /// [host] must be current coordinator broker for [consumerGroup].
  OffsetCommitRequest(KafkaClient client, KafkaHost host, this.consumerGroup,
      this.offsets, this.consumerGroupGenerationId, this.consumerId)
      : super(client, host);

  Future<OffsetCommitResponse> send() async {
    return this.client.send(host, this);
  }

  @override
  List<int> toBytes() {
    var builder = new KafkaBytesBuilder.withRequestHeader(
        apiKey, apiVersion, correlationId);

    var timestamp = new DateTime.now().millisecondsSinceEpoch;
    builder.addString(consumerGroup);
    builder.addInt32(consumerGroupGenerationId);
    builder.addString(consumerId);
    builder.addInt32(offsets.length);
    offsets.forEach((topicName, partitions) {
      builder.addString(topicName);
      builder.addInt32(partitions.length);
      partitions.forEach((p) {
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
  _createResponse(List<int> data) {
    return new OffsetCommitResponse.fromData(data, correlationId);
  }
}

class ConsumerOffset {
  final int partitionId;
  final int offset;
  final String metadata;
  final int errorCode;

  ConsumerOffset(this.partitionId, this.offset, this.metadata,
      [this.errorCode]);
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
