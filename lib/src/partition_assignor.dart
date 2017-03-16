import 'common.dart';

abstract class PartitionAssignor {
  Map<String, List<TopicPartition>> assign(Map<String, int> partitionsPerTopic,
      Map<String, Set<String>> memberSubscriptions);

  factory PartitionAssignor.forStrategy(String assignmentStrategy) {
    switch (assignmentStrategy) {
      case 'roundrobin':
        return new RoundRobinPartitionAssignor();
      default:
        throw new ArgumentError(
            'Unsupported assignment strategy "$assignmentStrategy" for PartitionAssignor.');
    }
  }
}

/// Partition assignor implementing simple "round-robin" algorithm.
///
/// It can only be used if the set of subscribed topics is identical for every
/// member within consumer group.
class RoundRobinPartitionAssignor implements PartitionAssignor {
  @override
  Map<String, List<TopicPartition>> assign(Map<String, int> partitionsPerTopic,
      Map<String, Set<String>> memberSubscriptions) {
    var topics = new Set<String>();
    memberSubscriptions.values.forEach(topics.addAll);
    if (!memberSubscriptions.values
        .every((list) => list.length == topics.length)) {
      throw new StateError(
          'RoundRobinPartitionAssignor: All members must subscribe to the same topics. '
          'Subscriptions given: $memberSubscriptions.');
    }

    Map<String, List<TopicPartition>> assignments = new Map.fromIterable(
        memberSubscriptions.keys,
        value: (_) => new List());

    var offset = 0;
    for (var topic in partitionsPerTopic.keys) {
      List<TopicPartition> partitions = new List.generate(
          partitionsPerTopic[topic], (_) => new TopicPartition(topic, _));
      for (var p in partitions) {
        var k = (offset + p.partition) % memberSubscriptions.keys.length;
        var memberId = memberSubscriptions.keys.elementAt(k);
        assignments[memberId].add(p);
      }
      offset += partitions.last.partition + 1;
    }

    return assignments;
  }
}
