import '../util/group_by.dart';
import '../util/tuple.dart';
import 'common.dart';
import 'errors.dart';
import 'io.dart';

/// Kafka ListOffsetRequest version 1.
class ListOffsetRequestV1 extends KRequest<ListOffsetResponseV1> {
  /// API key of this request.
  @override
  final int apiKey = 2;

  /// API version of this request.
  @override
  final int apiVersion = 1;

  /// Unique ID assigned to the host within Kafka cluster.
  final int replicaId;

  /// Map of topic-partitions to timestamps (in msecs)
  final Map<TopicPartition, int> topics;

  /// Creates new instance of OffsetRequest.
  ///
  /// The [replicaId] argument indicates unique ID assigned to the host within
  /// Kafka cluster.
  /// The [topics] argument is a `Map` where keys are instances of
  /// [TopicPartition] and values are integer timestamps (ms). Timestamps
  /// are used to ask for all entries before a certain time. Specify `-1` to
  /// receive the latest offset (i.e. the offset of the next coming message)
  /// and -2 to receive the earliest available offset.
  ListOffsetRequestV1(this.replicaId, this.topics);

  @override
  ResponseDecoder<ListOffsetResponseV1> get decoder =>
      const _ListOffsetResponseV1Decoder();

  @override
  RequestEncoder<KRequest> get encoder => const _ListOffsetRequestV1Encoder();
}

/// Kafka ListOffsetResponseV1.
class ListOffsetResponseV1 {
  /// Map of topics and list of partitions with offset details.
  final List<TopicOffsets> offsets;

  ListOffsetResponseV1(this.offsets) {
    var errorOffset = offsets.firstWhere(
        (_) => _.errorCode != KafkaServerError.NoError_,
        orElse: () => null);
    if (errorOffset != null) {
      throw new KafkaServerError.fromCode(errorOffset.errorCode, this);
    }
  }
}

/// Data structure representing offsets of particular topic-partition returned
/// by [ListOffsetRequestV1].
class TopicOffsets {
  final String topic;
  final int partition;
  final int errorCode;
  final int timestamp;
  final int offset;

  TopicOffsets(
      this.topic, this.partition, this.errorCode, this.timestamp, this.offset);

  @override
  toString() =>
      'TopicOffset[$topic-$partition, error: $errorCode, timestamp: $timestamp, offsets: $offset]';
}

class _ListOffsetRequestV1Encoder
    implements RequestEncoder<ListOffsetRequestV1> {
  const _ListOffsetRequestV1Encoder();

  @override
  List<int> encode(ListOffsetRequestV1 request) {
    var builder = new KafkaBytesBuilder();
    builder.addInt32(request.replicaId);

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

class _ListOffsetResponseV1Decoder
    implements ResponseDecoder<ListOffsetResponseV1> {
  const _ListOffsetResponseV1Decoder();

  @override
  ListOffsetResponseV1 decode(List<int> data) {
    var reader = new KafkaBytesReader.fromBytes(data);

    var count = reader.readInt32();
    var offsets = new List<TopicOffsets>();
    while (count > 0) {
      var topic = reader.readString();
      var partitionCount = reader.readInt32();
      while (partitionCount > 0) {
        var partition = reader.readInt32();
        var errorCode = reader.readInt16();
        var timestamp = reader.readInt64();
        var offset = reader.readInt64();
        offsets.add(
            new TopicOffsets(topic, partition, errorCode, timestamp, offset));
        partitionCount--;
      }
      count--;
    }

    return new ListOffsetResponseV1(offsets);
  }
}
