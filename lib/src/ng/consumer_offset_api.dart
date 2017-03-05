import 'io.dart';
import 'errors.dart';

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
}

/// Kafka OffsetFetchRequest.
///
/// Throws `GroupLoadInProgressError`, `NotCoordinatorForGroupError`,
/// `IllegalGenerationError`, `UnknownMemberIdError`, `TopicAuthorizationFailedError`,
/// `GroupAuthorizationFailedError`.
class OffsetFetchRequest extends KRequest<OffsetFetchResponse> {
  @override
  final int apiKey = 9;

  @override
  final int apiVersion = 1;

  /// Name of consumer group.
  final String group;

  /// Map of topic names and partition IDs to fetch offsets for.
  final Map<String, List<int>> topics;

  /// Creates new instance of [OffsetFetchRequest].
  OffsetFetchRequest(this.group, this.topics);

  @override
  ResponseDecoder<OffsetFetchResponse> get decoder =>
      const OffsetFetchResponseDecoder();

  @override
  RequestEncoder<KRequest> get encoder => const OffsetFetchRequestEncoder();
}

class OffsetFetchRequestEncoder implements RequestEncoder<OffsetFetchRequest> {
  const OffsetFetchRequestEncoder();

  @override
  List<int> encode(OffsetFetchRequest request) {
    var builder = new KafkaBytesBuilder();

    builder.addString(request.group);
    builder.addInt32(request.topics.length);
    request.topics.forEach((topicName, partitions) {
      builder.addString(topicName);
      builder.addInt32Array(partitions.toList());
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

class OffsetFetchResponseDecoder
    implements ResponseDecoder<OffsetFetchResponse> {
  const OffsetFetchResponseDecoder();

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
          var errorCode = _.readInt16();
          return new ConsumerOffset(
              topic, partition, offset, metadata, errorCode);
        });

        offsets.add(offset);
        partitionCount--;
      }
      count--;
    }

    return new OffsetFetchResponse(offsets);
  }
}
