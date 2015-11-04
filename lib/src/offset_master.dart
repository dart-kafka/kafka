part of kafka;

/// Master of Offsets (\m/).
///
/// Encapsulates auto-discovery logic for fetching topic offsets.
class OffsetMaster {
  final KafkaSession session;

  OffsetMaster(this.session);

  Future<List<TopicOffset>> fetchEarliest(Map<String, Set<int>> topicPartitions,
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
        requests[host].addTopicPartition(topic, p, -2, 1);
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
            return fetchEarliest(topicPartitions, refreshMetadata: true);
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

class TopicOffset {
  final String topicName;
  final int partitionId;
  final int offset;

  TopicOffset(this.topicName, this.partitionId, this.offset);
}
