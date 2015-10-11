part of kafka;

/// Compression types supported by Kafka.
enum KafkaCompression { none, gzip, snappy }

/// Base interface for all Kafka API requests.
abstract class KafkaRequest {
  static final _random = new Random();

  final KafkaClient client;
  final KafkaHost host;
  final int correlationId;

  KafkaRequest(this.client, this.host) : correlationId = _random.nextInt(65536);

  List<int> toBytes();

  dynamic _createResponse(List<int> data);
}

class TopicPartitions {
  final String topicName;
  final Set<int> partitions;

  TopicPartitions(this.topicName, this.partitions);
}
