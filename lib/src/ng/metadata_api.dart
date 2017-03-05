import 'common.dart';
import 'errors.dart';
import 'io.dart';

/// Kafka MetadataRequest.
class MetadataRequest extends KRequest<MetadataResponse> {
  @override
  final int apiKey = 3;

  @override
  final int apiVersion = 0;

  @override
  final RequestEncoder<KRequest> encoder = const _MetadataRequestEncoder();

  @override
  final ResponseDecoder<MetadataResponse> decoder =
      const _MetadataResponseDecoder();

  final List<String> topicNames;

  /// Creates new MetadataRequest.
  ///
  /// If [topicNames] is not set it fetches information about all existing
  /// topics in the Kafka cluster.
  MetadataRequest([this.topicNames]);
}

class MetadataResponse {
  /// List of brokers in the Kafka cluster.
  final List<Broker> brokers;

  /// List of topics in the Kafka cluster.
  final List<TopicMetadata> topics;

  MetadataResponse(this.brokers, this.topics) {
    var errorTopic =
        topics.firstWhere((_) => _.error != Errors.NoError, orElse: () => null);
    if (errorTopic is TopicMetadata) {
      throw new KafkaError.fromCode(errorTopic.error, this);
    }
  }
}

class TopicMetadata {
  final int error;
  final String topic;
  final List<PartitionMetadata> partitions;

  TopicMetadata(this.error, this.topic, this.partitions);

  @override
  toString() => 'TopicMetadata{$topic, error: $error; $partitions}';
}

class PartitionMetadata {
  final int error;
  final int id;
  final int leader;
  final List<int> replicas;
  final List<int> inSyncReplicas;

  PartitionMetadata(
      this.error, this.id, this.leader, this.replicas, this.inSyncReplicas);

  @override
  toString() => 'Partition#$id{error: $error, '
      'leader: $leader, replicas: $replicas, isr: $inSyncReplicas}';
}

class _MetadataRequestEncoder implements RequestEncoder<MetadataRequest> {
  const _MetadataRequestEncoder();

  @override
  List<int> encode(MetadataRequest request) {
    var builder = new KafkaBytesBuilder();
    List<String> topics = request.topicNames ?? new List();
    builder.addStringArray(topics);
    return builder.takeBytes();
  }
}

class _MetadataResponseDecoder implements ResponseDecoder<MetadataResponse> {
  const _MetadataResponseDecoder();

  @override
  MetadataResponse decode(List<int> data) {
    var reader = new KafkaBytesReader.fromBytes(data);
    List<Broker> brokers = reader.readObjectArray((r) {
      return new Broker(r.readInt32(), r.readString(), r.readInt32());
    });

    var topics = reader.readObjectArray((r) {
      var errorCode = reader.readInt16();
      var topicName = reader.readString();

      List<PartitionMetadata> partitions = reader.readObjectArray((r) =>
          new PartitionMetadata(r.readInt16(), r.readInt32(), r.readInt32(),
              r.readInt32Array(), r.readInt32Array()));
      return new TopicMetadata(errorCode, topicName, partitions);
    });
    return new MetadataResponse(brokers, topics);
  }
}
