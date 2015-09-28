part of kafka;

/// ProduceRequest as defined in Kafka protocol specification.
///
/// _Important: this class will not perform any checks to validate that
/// messages in the payload can be published to particular Kafka broker since
/// this kind of behavior is handled on the Kafka protocol level and any failure
/// to publish a message due to incorrectly selected broker will result in Kafka
/// API error which can be handled separately._
class ProduceRequest extends KafkaRequest {
  /// API key of [ProduceRequest]
  final int apiKey = 0;

  /// API version of [ProduceRequest]
  final int apiVersion = 0;

  KafkaClient _client;
  KafkaHost _host;
  Map<String, Map<int, MessageSet>> _messages = new Map();

  /// Indicates how many acknowledgements the servers
  /// should receive before responding to the request.
  int requiredAcks;

  /// Provides a maximum time in milliseconds the server
  /// can await the receipt of the number of acknowledgements in [requiredAcks].
  int timeout;

  /// Kafka broker instance which would be a target of this request.
  KafkaHost get host => _host;

  /// Creates Kafka [ProduceRequest].
  ///
  /// The [requiredAcks] field indicates how many acknowledgements the servers
  /// should receive before responding to the request.
  /// The [timeout] field provides a maximum time in milliseconds the server
  /// can await the receipt of the number of acknowledgements in [requiredAcks].
  ProduceRequest(
      KafkaClient client, KafkaHost host, this.requiredAcks, this.timeout) {
    this._client = client;
    this._host = host;
  }

  /// Adds messages to be published by this [ProduceRequest] when it's sent.
  void addMessages(String topicName, int partitionId, List messages) {
    var partitions = _getTopicPartitions(topicName);
    if (!partitions.containsKey(partitionId)) {
      var messageSet = new MessageSet();
      messages.forEach((m) => messageSet.addMessage(new Message(m)));
      partitions[partitionId] = messageSet;
    } else {
      messages.forEach((m) {
        partitions[partitionId].addMessage(new Message(m));
      });
    }
  }

  /// Sends this request to selected Kafka broker ([host]).
  Future<ProduceResponse> send() async {
    var data = await _client.send(_host, this);
    return new ProduceResponse.fromData(data);
  }

  @override
  List<int> toBytes() {
    var builder = new KafkaBytesBuilder();
    _writeHeader(builder, apiKey, apiVersion, 0);
    builder.addInt16(requiredAcks);
    builder.addInt32(timeout);

    builder.addInt32(_messages.length);
    _messages.forEach((topicName, partitions) {
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

  Map<int, MessageSet> _getTopicPartitions(topicName) {
    if (_messages.containsKey(topicName) == false) {
      _messages[topicName] = new Map();
    }

    return _messages[topicName];
  }
}

/// Result of [ProduceRequest] as defined in Kafka protocol specification.
class ProduceResponse {
  List<TopicProduceResult> topics;
  ProduceResponse.fromData(List<int> data) {
    var reader = new KafkaBytesReader.fromBytes(data);
    var size = reader.readInt32();
    assert(size == data.length - 4);

    var correlationId = reader.readInt32(); // TODO verify correlationId

    this.topics = reader.readArray(
        KafkaType.object, (r) => new TopicProduceResult.readFrom(r));
  }
}

class TopicProduceResult {
  String topicName;
  List<PartitionProduceResult> partitions;

  TopicProduceResult.readFrom(KafkaBytesReader reader) {
    this.topicName = reader.readString();
    this.partitions = reader.readArray(
        KafkaType.object, (r) => new PartitionProduceResult.readFrom(r));
  }

  @override
  String toString() {
    return 'Topic: ${topicName}. Partitions: ${partitions}';
  }
}

class PartitionProduceResult {
  int partitionId;
  int errorCode;
  int offset;

  PartitionProduceResult.readFrom(KafkaBytesReader reader) {
    this.partitionId = reader.readInt32();
    this.errorCode = reader.readInt16();
    this.offset = reader.readInt64();
  }

  @override
  String toString() {
    return '(Partition: ${partitionId}, errorCode: ${errorCode}, offset: ${offset} )';
  }
}
