part of kafka;

/// Master of Offsets.
///
/// Encapsulates auto-discovery logic for fetching topic offsets.
class OffsetMaster {
  /// Instance of KafkaSession.
  final KafkaSession session;

  /// Creates new OffsetMaster.
  OffsetMaster(this.session);

  /// Returns earliest offsets for specified topics and partitions.
  Future<List<TopicOffset>> fetchEarliest(
      Map<String, Set<int>> topicPartitions) {
    return _fetch(topicPartitions, -2);
  }

  /// Returns latest offsets (that is the offset of next incoming message)
  /// for specified topics and partitions.
  ///
  /// These offsets are also called 'highWatermark' offsets in Kafka docs.
  Future<List<TopicOffset>> fetchLatest(Map<String, Set<int>> topicPartitions) {
    return _fetch(topicPartitions, -1);
  }

  Future<List<TopicOffset>> _fetch(
      Map<String, Set<int>> topicPartitions, int time,
      {refreshMetadata: false}) async {
    var meta = await session.getMetadata(topicPartitions.keys.toSet(),
        invalidateCache: refreshMetadata);
    var requests = new Map<Broker, OffsetRequest>();
    for (var topic in topicPartitions.keys) {
      var partitions = topicPartitions[topic];
      for (var p in partitions) {
        var leader = meta.getTopicMetadata(topic).getPartition(p).leader;
        var host = meta.getBroker(leader);
        requests.putIfAbsent(host, () => new OffsetRequest(leader));
        requests[host].addTopicPartition(topic, p, time, 1);
      }
    }

    try {
      List<Future<OffsetResponse>> futures = [];
      for (var host in requests.keys) {
//        futures.add(session.send(host, requests[host]));
      }

      List<OffsetResponse> responses = await Future.wait(futures);
      var offsets = responses.expand((_) => _.offsets);
      return offsets
          .map((_) =>
              new TopicOffset(_.topicName, _.partitionId, _.offsets.first))
          .toList();
    } on NotLeaderForPartitionError {
      if (!refreshMetadata) {
        return _fetch(topicPartitions, time, refreshMetadata: true);
      } else {
        rethrow;
      }
    }
  }
}

/// Represents an offset of particular topic and partition.
class TopicOffset {
  final String topicName;
  final int partitionId;
  final int offset;

  TopicOffset(this.topicName, this.partitionId, this.offset);

  /// Creates pseudo-offset which refers to earliest offset in this topic
  /// and partition.
  TopicOffset.earliest(this.topicName, this.partitionId) : offset = -2;

  /// Creates pseudo-offset which refers to latest offset in this topic and
  /// partition.
  TopicOffset.latest(this.topicName, this.partitionId) : offset = -1;

  /// Indicates whether this is an earliest pseudo-offset.
  bool get isEarliest => offset == -2;

  /// Indicates whether this is a latest pseudo-offset.
  bool get isLatest => offset == -1;
}
