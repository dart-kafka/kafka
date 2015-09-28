part of kafka;

/// Compression types supported by Kafka.
///
/// Indexes of the values map to the ones defined in Kafka protocol.
enum KafkaCompression { none, gzip, snappy }

/// Base interface for all Kafka API requests.
abstract class KafkaRequest {
  List<int> toBytes();

  void _writeHeader(KafkaBytesBuilder builder, int apiKey, int apiVersion,
      int correlationId) {
    builder
      ..addInt16(apiKey)
      ..addInt16(apiVersion)
      ..addInt32(correlationId)
      ..addString("dart-kafka-v1.0.0-dev");
  }
}
