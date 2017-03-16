import 'common.dart';
import 'consumer_offset_api.dart';
import 'errors.dart';
import 'io.dart';
import 'util/group_by.dart';

/// Kafka OffsetCommitRequest.
class OffsetCommitRequest extends KRequest<OffsetCommitResponse> {
  @override
  final int apiKey = ApiKey.offsetCommit;

  /// The name of consumer group.
  final String group;

  /// The generation ID of consumer group.
  final int generationId;

  /// The ID of consumer group member.
  final String consumerId;

  /// Time period in msec to retain the offset.
  final int retentionTime;

  /// List of consumer offsets to be committed.
  final List<ConsumerOffset> offsets;

  /// Creates new instance of OffsetCommit request.
  OffsetCommitRequest(this.group, this.offsets, this.generationId,
      this.consumerId, this.retentionTime);

  @override
  ResponseDecoder<OffsetCommitResponse> get decoder =>
      const _OffsetCommitResponseDecoder();

  @override
  RequestEncoder<KRequest> get encoder => const _OffsetCommitRequestEncoder();
}

/// OffsetCommitResponse.
class OffsetCommitResponse {
  final List<OffsetCommitResult> results;

  OffsetCommitResponse(this.results) {
    var errorResult = results.firstWhere((_) => _.error != Errors.NoError,
        orElse: () => null);
    if (errorResult != null)
      throw new KafkaError.fromCode(errorResult.error, this);
  }
}

/// Result of commiting a consumer offset.
class OffsetCommitResult {
  final String topic;
  final int partition;
  final int error;

  OffsetCommitResult(this.topic, this.partition, this.error);
}

class _OffsetCommitRequestEncoder
    implements RequestEncoder<OffsetCommitRequest> {
  const _OffsetCommitRequestEncoder();

  @override
  List<int> encode(OffsetCommitRequest request, int version) {
    assert(version == 2,
        'Only v2 of OffsetCommit request is supported by the client.');

    var builder = new KafkaBytesBuilder();
    Map<String, List<ConsumerOffset>> groupedByTopic =
        groupBy(request.offsets, (o) => o.topic);

    builder.addString(request.group);
    builder.addInt32(request.generationId);
    builder.addString(request.consumerId);
    builder.addInt64(request.retentionTime);
    builder.addInt32(groupedByTopic.length);
    groupedByTopic.forEach((topic, partitionOffsets) {
      builder.addString(topic);
      builder.addInt32(partitionOffsets.length);
      partitionOffsets.forEach((p) {
        builder.addInt32(p.partition);
        builder.addInt64(p.offset);
        builder.addString(p.metadata);
      });
    });

    return builder.takeBytes();
  }
}

class _OffsetCommitResponseDecoder
    implements ResponseDecoder<OffsetCommitResponse> {
  const _OffsetCommitResponseDecoder();

  @override
  OffsetCommitResponse decode(List<int> data) {
    var reader = new KafkaBytesReader.fromBytes(data);
    List<OffsetCommitResult> results = [];
    var count = reader.readInt32();

    while (count > 0) {
      var topic = reader.readString();
      var partitionCount = reader.readInt32();
      while (partitionCount > 0) {
        var partition = reader.readInt32();
        var error = reader.readInt16();
        results.add(new OffsetCommitResult(topic, partition, error));
        partitionCount--;
      }
      count--;
    }

    return new OffsetCommitResponse(results);
  }
}
