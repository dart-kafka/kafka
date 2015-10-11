part of kafka;

class KafkaHost {
  final String host;
  final int port;

  static final Map<String, KafkaHost> _instances = new Map();

  factory KafkaHost(String host, int port) {
    var key = '${host}:${port}';
    if (!_instances.containsKey(key)) {
      _instances[key] = new KafkaHost._internal(host, port);
    }
    return _instances[key];
  }

  KafkaHost._internal(this.host, this.port);
}

class KafkaConfig {}
