part of kafka;

/// High-level Kafka consumer class.
///
/// Provides convenience layer on top of Kafka's [FetchRequest],
/// [ConsumerMetadataRequest] and Offset Fetch / Commit API.
class Consumer {
  /// Instance of [KafkaClient] used to send requests.
  final KafkaClient client;

  /// Consumer group.
  final ConsumerGroup consumerGroup;

  /// Maximum amount of time in milliseconds to block waiting if insufficient
  /// data is available at the time the request is issued.
  final int maxWaitTime;

  /// Minimum number of bytes of messages that must be available
  /// to give a response.
  final int minBytes;

  final Map<KafkaHost, FetchRequest> _fetchRequests = new Map();

  /// Creates new consumer identified by [consumerGroup].
  Consumer(this.client, this.consumerGroup, this.maxWaitTime, this.minBytes);

  Future addTopicPartitions(TopicPartitions partitions) async {
    //
  }

  FetchRequest _getRequestForHost(KafkaHost host) {
    if (!_fetchRequests.containsKey(host)) {
      _fetchRequests[host] =
          new FetchRequest(client, host, maxWaitTime, minBytes);
    }
    return _fetchRequests[host];
  }
}
