part of kafka;

/// Kafka Metadata Request.
class MetadataRequest extends KafkaRequest {
  final int apiKey = 3;
  final int apiVersion = 0;

  KafkaClient _client;
  KafkaHost _host;
  List<String> topicNames;

  /// Creats new instance of Kafka MetadataRequest.
  ///
  /// If [topicNames] is omitted then metadata for all existing topics
  /// will be returned.
  MetadataRequest(this._client, this._host, [List<String> topicNames]) {
    this.topicNames = (topicNames == null) ? [] : topicNames;
  }

  /// Sends the request.
  Future<MetadataResponse> send() async {
    var data = await _client.send(_host, this);
    return new MetadataResponse.fromData(data);
  }

  @override
  List<int> toBytes() {
    var builder = new KafkaBytesBuilder();
    _writeHeader(builder, apiKey, apiVersion, 0);
    builder.addArray(this.topicNames, KafkaType.string);

    var body = builder.takeBytes();
    builder.addBytes(body);

    return builder.takeBytes();
  }
}

/// Response to Metadata Request.
class MetadataResponse {
  List<Broker> brokers;
  List<TopicMetadata> topicMetadata;

  MetadataResponse.fromData(List<int> data) {
    var reader = new KafkaBytesReader.fromBytes(data);
    var size = reader.readInt32();
    assert(size == data.length - 4);

    var correlationId = reader.readInt32(); // TODO verify correlationId

    this.brokers = reader.readArray(
        KafkaType.object, (reader) => new Broker.readFrom(reader));
    this.topicMetadata = reader.readArray(
        KafkaType.object, (reader) => new TopicMetadata.readFrom(reader));
  }

  /// Returns [Broker] by specified [nodeId]. If no broker found will
  /// throw [StateError].
  Broker getBroker(int nodeId) {
    return brokers.firstWhere((b) => b.nodeId == nodeId,
        orElse: () => throw new StateError(
            'No broker with ID ${nodeId} found in metadata.'));
  }

  TopicMetadata getTopicMetadata(String topicName) {
    return topicMetadata.firstWhere((topic) => topic.topicName == topicName,
        orElse: () =>
            throw new StateError('No topic ${topicName} found in metadata.'));
  }
}

/// Represents Kafka Broker data structure returned in MetadataResponse.
class Broker {
  int nodeId;
  String host;
  int port;

  Broker.readFrom(KafkaBytesReader reader) {
    this.nodeId = reader.readInt32();
    this.host = reader.readString();
    this.port = reader.readInt32();
  }

  @override
  String toString() {
    return "Broker(nodeId: ${nodeId.toString()}, host: ${host}, port: ${port.toString()})";
  }
}

/// Represents Kafka TopicMetadata data structure returned in MetadataResponse.
class TopicMetadata {
  int topicErrorCode;
  String topicName;
  List<PartitionMetadata> partitionsMetadata;

  TopicMetadata.readFrom(KafkaBytesReader reader) {
    topicErrorCode = reader.readInt16();
    topicName = reader.readString();
    partitionsMetadata = reader.readArray(
        KafkaType.object, (reader) => new PartitionMetadata.readFrom(reader));
  }

  PartitionMetadata getPartition(int partitionId) {
    return partitionsMetadata.firstWhere((p) => p.partitionId == partitionId,
        orElse: () => throw new StateError(
            'No partition ${partitionId} found in metadata for topic ${topicName}.'));
  }

  @override
  String toString() {
    return "TopicMetadata(errorCode: ${topicErrorCode.toString()}, name: ${topicName}, partitions: ${partitionsMetadata.toString()})";
  }
}

/// Represents Kafka PartitionMetadata data structure returned in MetadataResponse.
class PartitionMetadata {
  int partitionErrorCode;
  int partitionId;
  int leader;
  List<int> replicas;
  List<int> inSyncReplicas;

  PartitionMetadata.readFrom(KafkaBytesReader reader) {
    partitionErrorCode = reader.readInt16();
    partitionId = reader.readInt32();
    leader = reader.readInt32();
    replicas = reader.readArray(KafkaType.int32);
    inSyncReplicas = reader.readArray(KafkaType.int32);
  }
}
