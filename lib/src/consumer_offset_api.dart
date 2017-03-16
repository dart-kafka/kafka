import 'common.dart';
import 'errors.dart';
import 'io.dart';
import 'util/group_by.dart';

/// Consumer position in particular [topic] and [partition].
class ConsumerOffset {
  /// The name of the topic.
  final String topic;

  /// The partition number;
  final int partition;

  /// The offset of last message handled by a consumer.
  final int offset;

  /// User-defined metadata associated with this offset.
  final String metadata;

  /// The error code returned by the server.
  final int error;

  ConsumerOffset(this.topic, this.partition, this.offset, this.metadata,
      [this.error]);

  TopicPartition get topicPartition => new TopicPartition(topic, partition);

  /// Copies this offset and overwrites [offset] and [metadata] if provided.
  ConsumerOffset copy({int offset, String metadata}) {
    assert(offset != null);
    return new ConsumerOffset(topic, partition, offset, metadata);
  }

  @override
  String toString() =>
      'ConsumerOffset{partition: $topic:$partition, offset: $offset, metadata: $metadata';
}

/// Kafka OffsetFetchRequest.
///
/// Throws `GroupLoadInProgressError`, `NotCoordinatorForGroupError`,
/// `IllegalGenerationError`, `UnknownMemberIdError`, `TopicAuthorizationFailedError`,
/// `GroupAuthorizationFailedError`.
class OffsetFetchRequest extends KRequest<OffsetFetchResponse> {
  @override
  final int apiKey = ApiKey.offsetFetch;

  /// Name of consumer group.
  final String group;

  /// Map of topic names and partition IDs to fetch offsets for.
  final List<TopicPartition> partitions;

  /// Creates new instance of [OffsetFetchRequest].
  OffsetFetchRequest(this.group, this.partitions);

  @override
  ResponseDecoder<OffsetFetchResponse> get decoder =>
      const _OffsetFetchResponseDecoder();

  @override
  RequestEncoder<KRequest> get encoder => const _OffsetFetchRequestEncoder();
}

class _OffsetFetchRequestEncoder implements RequestEncoder<OffsetFetchRequest> {
  const _OffsetFetchRequestEncoder();

  @override
  List<int> encode(OffsetFetchRequest request, int version) {
    assert(version == 1,
        'Only v1 of OffsetFetch request supported by the client.');
    var builder = new KafkaBytesBuilder();

    builder.addString(request.group);
    Map<String, List<TopicPartition>> grouped = groupBy<String, TopicPartition>(
        request.partitions, (partition) => partition.topic);

    builder.addInt32(grouped.length);
    grouped.forEach((topic, partitions) {
      builder.addString(topic);
      var partitionIds =
          partitions.map((_) => _.partition).toList(growable: false);
      builder.addInt32Array(partitionIds);
    });

    return builder.takeBytes();
  }
}

/// Kafka OffsetFetchResponse.
class OffsetFetchResponse {
  final List<ConsumerOffset> offsets;

  OffsetFetchResponse(this.offsets) {
    var errors = offsets.map((_) => _.error).where((_) => _ != Errors.NoError);
    if (errors.isNotEmpty) {
      throw new KafkaError.fromCode(errors.first, this);
    }
  }
}

class _OffsetFetchResponseDecoder
    implements ResponseDecoder<OffsetFetchResponse> {
  const _OffsetFetchResponseDecoder();

  @override
  OffsetFetchResponse decode(List<int> data) {
    List<ConsumerOffset> offsets = [];
    var reader = new KafkaBytesReader.fromBytes(data);

    var count = reader.readInt32();
    while (count > 0) {
      String topic = reader.readString();
      int partitionCount = reader.readInt32();
      while (partitionCount > 0) {
        ConsumerOffset offset = reader.readObject((_) {
          var partition = _.readInt32();
          var offset = _.readInt64();
          var metadata = _.readString();
          var error = _.readInt16();
          return new ConsumerOffset(topic, partition, offset, metadata, error);
        });

        offsets.add(offset);
        partitionCount--;
      }
      count--;
    }

    return new OffsetFetchResponse(offsets);
  }
}
