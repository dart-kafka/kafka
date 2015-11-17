part of kafka;

/// Message Fetcher.
///
/// Main difference to [Consumer] is that this class does not store it's state.
/// Fetcher just reads messages starting from the specified offsets.
class Fetcher {
  /// Instance of Kafka session.
  final KafkaSession session;

  /// Offsets to start from.
  final List<TopicOffset> topicOffsets;

  Fetcher(this.session, this.topicOffsets);
}
