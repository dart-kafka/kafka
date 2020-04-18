import 'dart:async';
import 'common.dart';
import 'session.dart';
import 'list_offset_api.dart';

/// Master of Offsets.
///
/// Encapsulates auto-discovery logic for fetching topic offsets.
class OffsetMaster {
  /// The session used by this OffsetMaster.
  final Session session;

  /// Creates new OffsetMaster.
  OffsetMaster(this.session);

  /// Returns earliest offsets for specified topics and partitions.
  Future<List<TopicOffset>> fetchEarliest(List<TopicPartition> partitions) {
    return _fetch(partitions, -2);
  }

  /// Returns latest offsets (that is the offset of next incoming message)
  /// for specified topics and partitions.
  ///
  /// These offsets are also known as 'high watermark' offsets.
  Future<List<TopicOffset>> fetchLatest(List<TopicPartition> partitions) {
    return _fetch(partitions, -1);
  }

  Future<List<TopicOffset>> _fetch(
      List<TopicPartition> partitions, int time) async {
    var topics = partitions.map((_) => _.topic).toSet();
    var meta =
        await session.metadata.fetchTopics(topics.toList(growable: false));
    var requests = new Map<Broker, List<TopicPartition>>();
    var brokers = meta.brokers;
    for (var p in partitions) {
      var leaderId = meta[p.topic].partitions[p.partition].leader;
      var broker = brokers[leaderId];
      requests.putIfAbsent(broker, () => new List());
      requests[broker].add(p);
    }

    var offsets = new List<TopicOffset>();
    for (var host in requests.keys) {
      var fetchInfo = new Map<TopicPartition, int>.fromIterable(requests[host],
          value: (partition) => time);
      var request = new ListOffsetRequest(fetchInfo);
      ListOffsetResponse response =
          await session.send(request, host.host, host.port);
      offsets.addAll(response.offsets);
    }

    return offsets;
  }
}
