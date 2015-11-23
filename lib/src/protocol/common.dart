part of kafka.protocol;

/// Base interface for all Kafka API requests.
abstract class KafkaRequest {
  static final _random = new Random();

  final int correlationId;

  KafkaRequest() : correlationId = _random.nextInt(65536);

  List<int> toBytes();

  dynamic createResponse(List<int> data);
}
