import 'common.dart';
import 'errors.dart';
import 'io.dart';
import 'util/group_by.dart';
import 'util/tuple.dart';

/// Kafka ListOffsetRequest.
class ListOffsetRequest extends KRequest<ListOffsetResponse> {
  @override
  final int apiKey = ApiKey.offsets;

  /// Unique ID assigned to the host within Kafka cluster. Regular consumers
  /// should always pass `-1`.
  static const int replicaId = -1;

  /// Map of topic-partitions to timestamps (in msecs)
  final Map<TopicPartition, int> topics;

  /// Creates new instance of OffsetRequest.
  ///
  /// The [topics] argument is a `Map` where keys are instances of
  /// [TopicPartition] and values are integer timestamps (ms). Timestamps
  /// are used to ask for all entries before a certain time. Specify `-1` to
  /// receive the latest offset (i.e. the offset of the next coming message)
  /// and -2 to receive the earliest available offset.
  ListOffsetRequest(this.topics);

  @override
  ResponseDecoder<ListOffsetResponse> get decoder =>
      const _ListOffsetResponseDecoder();

  @override
  RequestEncoder<KRequest> get encoder => const _ListOffsetRequestEncoder();
}

/// Kafka ListOffsetResponse.
class ListOffsetResponse {
  /// List of offsets for each topic-partition.
  final List<TopicOffset> offsets;

  ListOffsetResponse(this.offsets) {
    var errorOffset = offsets.firstWhere((_) => _.error != Errors.NoError,
        orElse: () => null);
    if (errorOffset != null) {
      throw new KafkaError.fromCode(errorOffset.error, this);
    }
  }
}

/// Data structure representing offsets of particular topic-partition returned
/// by [ListOffsetRequest].
class TopicOffset {
  final String topic;
  final int partition;
  final int error;
  final int timestamp;
  final int offset;

  TopicOffset(
      this.topic, this.partition, this.error, this.timestamp, this.offset);

  @override
  toString() =>
      'TopicOffset{$topic-$partition, error: $error, timestamp: $timestamp, offset: $offset}';
}

class _ListOffsetRequestEncoder implements RequestEncoder<ListOffsetRequest> {
  const _ListOffsetRequestEncoder();

  @override
  List<int> encode(ListOffsetRequest request, int version) {
    assert(version == 1,
        'Only v1 of ListOffset request is supported by the client.');
    var builder = new KafkaBytesBuilder();
    builder.addInt32(ListOffsetRequest.replicaId);

    // <topic, partition, timestamp>
    List<Tuple3<String, int, int>> items = request.topics.keys.map((_) {
      return tuple3(_.topic, _.partition, request.topics[_]);
    }).toList(growable: false);

    Map<String, List<Tuple3<String, int, int>>> groupedByTopic =
        groupBy(items, (_) => _.$1);

    builder.addInt32(groupedByTopic.length);
    groupedByTopic.forEach((topic, partitions) {
      builder.addString(topic);
      builder.addInt32(partitions.length);
      partitions.forEach((p) {
        builder.addInt32(p.$2);
        builder.addInt64(p.$3);
      });
    });

    return builder.takeBytes();
  }
}

class _ListOffsetResponseDecoder
    implements ResponseDecoder<ListOffsetResponse> {
  const _ListOffsetResponseDecoder();

  @override
  ListOffsetResponse decode(List<int> data) {
    var reader = new KafkaBytesReader.fromBytes(data);

    var count = reader.readInt32();
    var offsets = new List<TopicOffset>();
    while (count > 0) {
      var topic = reader.readString();
      var partitionCount = reader.readInt32();
      while (partitionCount > 0) {
        var partition = reader.readInt32();
        var error = reader.readInt16();
        var timestamp = reader.readInt64();
        var offset = reader.readInt64();
        offsets
            .add(new TopicOffset(topic, partition, error, timestamp, offset));
        partitionCount--;
      }
      count--;
    }

    return new ListOffsetResponse(offsets);
  }
}
