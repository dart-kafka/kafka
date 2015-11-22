part of kafka;

/// Compression types supported by Kafka.
enum KafkaCompression { none, gzip, snappy }

/// Base interface for all Kafka API requests.
abstract class KafkaRequest {
  static final _random = new Random();

  final int correlationId;

  KafkaRequest() : correlationId = _random.nextInt(65536);

  List<int> toBytes();

  dynamic _createResponse(List<int> data);
}

/// Represents individual Kafka broker identified by host and port.
///
// TODO: consolidate with [Broker] (maybe?).
class KafkaHost {
  /// Host name or IP address of this Kafka broker.
  final String host;

  /// Port number of this Kafka broker.
  final int port;

  static final Map<String, KafkaHost> _instances = new Map();

  /// Creates instance of Kafka broker identified by [host] and [port].
  factory KafkaHost(String host, int port) {
    var key = '${host}:${port}';
    if (!_instances.containsKey(key)) {
      _instances[key] = new KafkaHost._internal(host, port);
    }
    return _instances[key];
  }

  KafkaHost._internal(this.host, this.port);
}
