part of kafka.common;

Map<dynamic, dynamic> groupBy(Iterable list, f(element)) {
  var grouped = new Map();
  for (var e in list) {
    var key = f(e);
    if (!grouped.containsKey(key)) {
      grouped[key] = new List();
    }
    grouped[key].add(e);
  }

  return grouped;
}

/// Data structure representing consumer offset.
class ConsumerOffset {
  final String topicName;
  final int partitionId;
  final int offset;
  final String metadata;
  final int errorCode;

  ConsumerOffset(this.topicName, this.partitionId, this.offset, this.metadata,
      [this.errorCode]);
}
