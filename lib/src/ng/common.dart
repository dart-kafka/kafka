class Broker {
  final int id;
  final String host;
  final int port;

  Broker(this.id, this.host, this.port);

  @override
  String toString() => 'Broker($id, $host:$port)';
}

class TopicPartition {
  final String topicName;
  final int partitionId;

  static final Map<String, TopicPartition> _cache = new Map();

  TopicPartition._(this.topicName, this.partitionId);

  factory TopicPartition(String topicName, int partitionId) {
    var key = topicName + partitionId.toString();
    if (!_cache.containsKey(key)) {
      _cache[key] = new TopicPartition._(topicName, partitionId);
    }

    return _cache[key];
  }

  @override
  bool operator ==(other) {
    return (other.topicName == topicName && other.partitionId == partitionId);
  }

  @override
  int get hashCode => (topicName + partitionId.toString()).hashCode;

  @override
  String toString() => "TopicPartition($topicName, $partitionId)";
}
