part of kafka.protocol;

/// Kafka ProduceRequest.
class ProduceRequest extends KafkaRequest {
  /// API key of [ProduceRequest]
  final int apiKey = 0;

  /// API version of [ProduceRequest]
  final int apiVersion = 0;

  /// Indicates how many acknowledgements the servers
  /// should receive before responding to the request.
  final int requiredAcks;

  /// Provides a maximum time in milliseconds the server
  /// can await the receipt of the number of acknowledgements in [requiredAcks].
  final int timeout;

  /// List of produce envelopes containing messages to be published.
  final List<ProduceEnvelope> messages;

  /// Creates Kafka [ProduceRequest].
  ///
  /// The [requiredAcks] field indicates how many acknowledgements the servers
  /// should receive before responding to the request.
  /// The [timeout] field provides a maximum time in milliseconds the server
  /// can await the receipt of the number of acknowledgements in [requiredAcks].
  ProduceRequest(this.requiredAcks, this.timeout, this.messages) : super();

  @override
  List<int> toBytes() {
    var builder = new KafkaBytesBuilder.withRequestHeader(
        apiKey, apiVersion, correlationId);
    builder.addInt16(requiredAcks);
    builder.addInt32(timeout);

    Map<String, Map<int, MessageSet>> messageSets = new Map();
    for (var envelope in messages) {
      if (!messageSets.containsKey(envelope.topicName)) {
        messageSets[envelope.topicName] = new Map<int, MessageSet>();
      }
      messageSets[envelope.topicName][envelope.partitionId] =
          new MessageSet.build(envelope);
    }

    builder.addInt32(messageSets.length);
    messageSets.forEach((topicName, partitions) {
      builder.addString(topicName);
      builder.addInt32(partitions.length);
      partitions.forEach((partitionId, messageSet) {
        builder.addInt32(partitionId);
        var messageData = messageSet.toBytes();
        builder.addInt32(messageData.length);
        builder.addRaw(messageData);
      });
    });

    var body = builder.takeBytes();
    builder.addBytes(body);

    return builder.takeBytes();
  }

  @override
  createResponse(List<int> data) {
    return new ProduceResponse.fromBytes(data);
  }
}

/// Kafka ProduceResponse.
class ProduceResponse {
  /// List of produce results for each topic-partition.
  final List<TopicProduceResult> results;

  ProduceResponse._(this.results);

  /// Creates response from the provided bytes [data].
  factory ProduceResponse.fromBytes(List<int> data) {
    var reader = new KafkaBytesReader.fromBytes(data);
    var size = reader.readInt32();
    assert(size == data.length - 4);

    reader.readInt32(); // correlationId
    var results = new List<TopicProduceResult>();
    var topicCount = reader.readInt32();
    while (topicCount > 0) {
      var topicName = reader.readString();
      var partitionCount = reader.readInt32();
      while (partitionCount > 0) {
        var partitionId = reader.readInt32();
        var errorCode = reader.readInt16();
        var offset = reader.readInt64();
        results.add(new TopicProduceResult._(
            topicName, partitionId, errorCode, offset));
        partitionCount--;
      }
      topicCount--;
    }
    return new ProduceResponse._(results);
  }
}

/// Data structure representing result of producing messages with
/// [ProduceRequest].
class TopicProduceResult {
  /// Name of the topic.
  final String topicName;

  /// ID of the partition.
  final int partitionId;

  /// Error code returned by the server.
  final int errorCode;

  /// Offset of the first message.
  final int offset;

  TopicProduceResult._(
      this.topicName, this.partitionId, this.errorCode, this.offset);

  @override
  String toString() =>
      'Topic: ${topicName}, partition: ${partitionId}, errorCode: ${errorCode}, offset: ${offset}';
}
