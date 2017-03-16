import 'package:quiver/core.dart';

import 'util/tuple.dart';

/// Numeric codes of Kafka API requests.
class ApiKey {
  static const produce = 0;
  static const fetch = 1;
  static const offsets = 2;
  static const metadata = 3;
  static const leaderAndIsr = 4;
  static const stopReplica = 5;
  static const updateMetadata = 6;
  static const controlledShutdown = 7;
  static const offsetCommit = 8;
  static const offsetFetch = 9;
  static const groupCoordinator = 10;
  static const joinGroup = 11;
  static const heartbeat = 12;
  static const leaveGroup = 13;
  static const syncGroup = 14;
  static const describeGroups = 15;
  static const listGroups = 16;
  static const saslHandshake = 17;
  static const apiVersions = 18;
  static const createTopics = 19;
  static const deleteTopics = 20;
}

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
