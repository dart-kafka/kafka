part of kafka.common;

/// Represents single node in a Kafka cluster.
class Broker {
  /// Unique ID of this broker within cluster.
  final int id;

  /// Host name or IP address of this broker.
  final String host;

  /// Port number of this broker.
  final int port;

  static final Map<String, Broker> _instances = new Map();

  /// Creates new instance of Kafka broker.
  factory Broker(int id, String host, int port) {
    var key = '${host}:${port}';
    if (!_instances.containsKey(key)) {
      _instances[key] = new Broker._(id, host, port);
    } else {
      if (_instances[key].id != id) throw new StateError('Broker ID mismatch.');
    }

    return _instances[key];
  }

  Broker._(this.id, this.host, this.port);
}
