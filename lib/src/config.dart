part of kafka;

/// Represents individual Kafka broker identified by host and port.
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
