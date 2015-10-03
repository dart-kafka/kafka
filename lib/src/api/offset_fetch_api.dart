part of kafka;

/// OffsetFetchRequest as defined in Kafka protocol.
///
/// This is a low-level API object. While it is posible to use this request
/// directly it is recommended to rely on high-level [KafkaConsumer] class which
/// encapsulates a lot of details about dealing with metadata and offsets.
class OffsetFetchRequest extends KafkaRequest {
  /// API key of [OffsetFetchRequest]
  final int apiKey = 9;

  /// API version of [OffsetFetchRequest].
  final int apiVersion = 1;

  /// Name of consumer group.
  final String consumerGroup;

  /// Map of topics and partitions
  final Map<String, List<int>> topics = new Map();

  /// Creates new instance of [OffsetFetchRequest].
  ///
  /// [host] must be current coordinator broker for [consumerGroup].
  OffsetFetchRequest(KafkaClient client, KafkaHost host, this.consumerGroup)
      : super(client, host);

  /// Adds topic-partitions to this request.
  void addTopicPartitions(String topicName, List<int> partitions) {
    if (!topics.containsKey(topicName)) {
      topics[topicName] = new List();
    }
    topics[topicName].addAll(partitions);
  }

  Future<OffsetFetchResponse> send() async {
    var data = await this.client.send(host, this);
    return new OffsetFetchResponse.fromData(data, correlationId);
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
}

/// Result of [OffsetFetchResponse] as defined in Kafka protocol.
class OffsetFetchResponse {
  final Map<String, List<ConsumerPartitionOffsetInfo>> topics = new Map();

  OffsetFetchResponse.fromData(List<int> data, int correlationId) {
    var reader = new KafkaBytesReader.fromBytes(data);
    var size = reader.readInt32();
    assert(size == data.length - 4);

    var receivedCorrelationId = reader.readInt32();
    if (receivedCorrelationId != correlationId) {
      throw new CorrelationIdMismatchError();
    }

    var count = reader.readInt32();
    while (count > 0) {
      var topicName = reader.readString();
      topics[topicName] = new List();
      var partitionCount = reader.readInt32();
      while (partitionCount > 0) {
        topics[topicName].add(ConsumerPartitionOffsetInfo.readFrom(reader));
        partitionCount--;
      }
      count--;
    }
  }
}

/// Information about consumer offset on particular topic partition.
class ConsumerPartitionOffsetInfo {
  final int partitionId;
  final int offset;
  final String metadata;
  final int errorCode;
  ConsumerPartitionOffsetInfo(
      this.partitionId, this.offset, this.metadata, this.errorCode);

  /// Reads data from [reader] and creates new instace of ConsumerPartitionOffsetInfo.
  static ConsumerPartitionOffsetInfo readFrom(KafkaBytesReader reader) {
    var id = reader.readInt32();
    var offset = reader.readInt64();
    var metadata = reader.readString();
    var errorCode = reader.readInt16();
    return new ConsumerPartitionOffsetInfo(id, offset, metadata, errorCode);
  }
}
