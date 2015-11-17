part of kafka;

/// Master of Offsets (\m/).
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
    var meta = await session.getMetadata(invalidateCache: refreshMetadata);
    var requests = new Map<KafkaHost, OffsetRequest>();
    for (var topic in topicPartitions.keys) {
      var partitions = topicPartitions[topic];
      for (var p in partitions) {
        var leader = meta.getTopicMetadata(topic).getPartition(p).leader;
        var host = meta.getBroker(leader).toKafkaHost();
        if (!requests.containsKey(host)) {
          requests[host] = new OffsetRequest(session, host, leader);
        }
        requests[host].addTopicPartition(topic, p, time, 1);
      }
    }

    var offsets = new List<TopicOffset>();
    for (var request in requests.values) {
      var response = await request.send();
      for (var topic in response.topics.keys) {
        var partitions = response.topics[topic];
        for (var p in partitions) {
          if (p.errorCode == KafkaApiError.NotLeaderForPartition &&
              refreshMetadata == false) {
            // Refresh metadata and try again.
            return _fetch(topicPartitions, time, refreshMetadata: true);
          }

          if (p.errorCode != KafkaApiError.NoError) {
            throw new KafkaApiError.fromErrorCode(p.errorCode);
          }
          offsets.add(new TopicOffset(topic, p.partitionId, p.offsets.first));
        }
      }
    }

    return offsets;
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
}
