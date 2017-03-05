import '../util/group_by.dart';
import 'consumer_offset_api.dart';
import 'errors.dart';
import 'io.dart';

/// Kafka OffsetCommitRequest.
class OffsetCommitRequestV2 extends KRequest<OffsetCommitResponseV2> {
  /// API key of this request.
  @override
  final int apiKey = 8;

  /// API version of this request.
  @override
  final int apiVersion = 2;

  /// Name of the consumer group.
  final String consumerGroup;

  /// Generation ID of the consumer group.
  final int generationId;

  /// The ID of consumer group member.
  final String consumerId;

  /// Time period in msec to retain the offset.
  final int retentionTime;

  /// List of consumer offsets to be committed.
  final List<ConsumerOffset> offsets;

  /// Creates new instance of OffsetCommit request.
  OffsetCommitRequestV2(this.consumerGroup, this.offsets, this.generationId,
      this.consumerId, this.retentionTime);

  @override
  ResponseDecoder<OffsetCommitResponseV2> get decoder =>
      const _OffsetCommitResponseV2Decoder();

  @override
  RequestEncoder<KRequest> get encoder => const _OffsetCommitRequestV2Encoder();
}

/// OffsetCommitResponse.
class OffsetCommitResponseV2 {
  final List<OffsetCommitResult> results;

  OffsetCommitResponseV2(this.results) {
    var errorResult = results.firstWhere((_) => _.errorCode != Errors.NoError,
        orElse: () => null);
    if (errorResult != null) {
      throw new KafkaError.fromCode(errorResult.errorCode, this);
    }
  }
}

/// Result of commiting a consumer offset.
class OffsetCommitResult {
  final String topic;
  final int partition;
  final int errorCode;

  OffsetCommitResult(this.topic, this.partition, this.errorCode);
}

class _OffsetCommitRequestV2Encoder
    implements RequestEncoder<OffsetCommitRequestV2> {
  const _OffsetCommitRequestV2Encoder();

  @override
  List<int> encode(OffsetCommitRequestV2 request) {
    var builder = new KafkaBytesBuilder();

    Map<String, List<ConsumerOffset>> groupedByTopic =
        groupBy(request.offsets, (o) => o.topic);

    builder.addString(request.consumerGroup);
    builder.addInt32(request.generationId);
    builder.addString(request.consumerId);
    builder.addInt64(request.retentionTime);
    builder.addInt32(groupedByTopic.length);
    groupedByTopic.forEach((topicName, partitionOffsets) {
      builder.addString(topicName);
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

class _OffsetCommitResponseV2Decoder
    implements ResponseDecoder<OffsetCommitResponseV2> {
  const _OffsetCommitResponseV2Decoder();

  @override
  OffsetCommitResponseV2 decode(List<int> data) {
    var reader = new KafkaBytesReader.fromBytes(data);
    List<OffsetCommitResult> results = [];
    var count = reader.readInt32();

    while (count > 0) {
      var topicName = reader.readString();
      var partitionCount = reader.readInt32();
      while (partitionCount > 0) {
        var partitionId = reader.readInt32();
        var errorCode = reader.readInt16();
        results.add(new OffsetCommitResult(topicName, partitionId, errorCode));
        partitionCount--;
      }
      count--;
    }

    return new OffsetCommitResponseV2(results);
  }
}
