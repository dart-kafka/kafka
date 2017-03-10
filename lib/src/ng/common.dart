import 'package:quiver/core.dart';

import '../util/tuple.dart';

/// Represents single broker in Kafka cluster.
class Broker {
  /// The unique identifier of this broker.
  final int id;

  /// The hostname of this broker.
  final String host;

  /// The port number this broker accepts connections on.
  final int port;

  Broker._(this.id, this.host, this.port);

  static final Map<Tuple3, Broker> _cache = new Map();

  factory Broker(int id, String host, int port) {
    var key = tuple3(id, host, port);
    if (!_cache.containsKey(key)) {
      _cache[key] = new Broker._(id, host, port);
    }

    return _cache[key];
  }

  @override
  int get hashCode => hash3(id, host, port);

  @override
  bool operator ==(o) =>
      o is Broker && o.id == id && o.host == host && o.port == port;

  @override
  String toString() => 'Broker{$id, $host:$port}';
}

/// Represents one partition in a topic.
class TopicPartition {
  /// The name of Kafka topic.
  final String topic;

  /// The partition ID.
  final int partition;

  static final Map<int, TopicPartition> _cache = new Map();

  TopicPartition._(this.topic, this.partition);

  factory TopicPartition(String topic, int partition) {
    var key = hash2(topic, partition);
    if (!_cache.containsKey(key)) {
      _cache[key] = new TopicPartition._(topic, partition);
    }

    return _cache[key];
  }

  @override
  bool operator ==(o) {
    return (o is TopicPartition &&
        o.topic == topic &&
        o.partition == partition);
  }

  @override
  int get hashCode => hash2(topic, partition);

  @override
  String toString() => "TopicPartition{$topic:$partition}";
}
