import 'package:quiver/core.dart';

import '../util/tuple.dart';

class Broker {
  final int id;
  final String host;
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
  String toString() => 'Broker($id, $host:$port)';
}

class TopicPartition {
  final String topic;
  final int partition;

  static final Map<String, TopicPartition> _cache = new Map();

  TopicPartition._(this.topic, this.partition);

  factory TopicPartition(String topicName, int partitionId) {
    var key = topicName + partitionId.toString();
    if (!_cache.containsKey(key)) {
      _cache[key] = new TopicPartition._(topicName, partitionId);
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
  String toString() => "TopicPartition($topic, $partition)";
}
