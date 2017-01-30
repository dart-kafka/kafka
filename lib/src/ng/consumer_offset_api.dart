import 'io.dart';
import 'errors.dart';

/// Value object to store consumer offset information.
class ConsumerOffset {
  final String topicName;
  final int partitionId;
  final int offset;
  final String metadata;
  final int errorCode;

  ConsumerOffset(this.topicName, this.partitionId, this.offset, this.metadata,
      [this.errorCode]);
}

/// Kafka OffsetFetchRequest.
///
/// Throws `GroupLoadInProgressError`, `NotCoordinatorForGroupError`,
/// `IllegalGenerationError`, `UnknownMemberIdError`, `TopicAuthorizationFailedError`,
/// `GroupAuthorizationFailedError`.
class OffsetFetchRequestV1 extends KRequest<OffsetFetchResponseV1> {
  @override
  final int apiKey = 9;
  @override
  final int apiVersion = 1;

  /// Name of consumer group.
  final String groupName;

  /// Map of topic names and partition IDs to fetch offsets for.
  final Map<String, Set<int>> topics;

  /// Creates new instance of [OffsetFetchRequestV0].
  OffsetFetchRequestV1(this.groupName, this.topics);

  @override
  ResponseDecoder<OffsetFetchResponseV1> get decoder =>
      new OffsetFetchResponseV1Decoder();

  @override
  RequestEncoder<KRequest> get encoder => new OffsetFetchRequestV1Encoder();
}

class OffsetFetchRequestV1Encoder
    implements RequestEncoder<OffsetFetchRequestV1> {
  @override
  List<int> encode(OffsetFetchRequestV1 request) {
    var builder = new KafkaBytesBuilder();

    builder.addString(request.groupName);
    builder.addInt32(request.topics.length);
    request.topics.forEach((topicName, partitions) {
      builder.addString(topicName);
      builder.addInt32Array(partitions.toList());
    });

    return builder.takeBytes();
  }
}

/// Kafka OffsetFetchResponseV1.
class OffsetFetchResponseV1 {
  final List<ConsumerOffset> offsets;

  OffsetFetchResponseV1(this.offsets) {
    var errors = offsets
        .map((_) => _.errorCode)
        .where((_) => _ != KafkaServerError.NoError_);
    if (errors.isNotEmpty) {
      throw new KafkaServerError.fromCode(errors.first, this);
    }
  }
}

class OffsetFetchResponseV1Decoder
    implements ResponseDecoder<OffsetFetchResponseV1> {
  @override
  OffsetFetchResponseV1 decode(List<int> data) {
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

    return new OffsetFetchResponseV1(offsets);
  }
}
