import 'common.dart';
import 'io.dart';
import 'errors.dart';

class MetadataRequestV0 extends KRequest<MetadataResponseV0> {
  @override
  final int apiKey = 3;

  @override
  final int apiVersion = 0;

  @override
  final RequestEncoder<KRequest> encoder = new _MetadataRequestV0Encoder();

  @override
  final ResponseDecoder<MetadataResponseV0> decoder =
      new _MetadataResponseV0Decoder();

  final List<String> topicNames;

  MetadataRequestV0([this.topicNames]);
}

class MetadataResponseV0 {
  final List<Broker> brokers;
  final List<TopicMetadata> topics;

  MetadataResponseV0(this.brokers, this.topics) {
    var errorTopic = topics.firstWhere(
        (_) => _.errorCode != KafkaServerError.NoError_,
        orElse: () => null);
    if (errorTopic is TopicMetadata) {
      throw new KafkaServerError.fromCode(errorTopic.errorCode, this);
    }
  }
}

class TopicMetadata {
  final int errorCode;
  final String topicName;
  final List<PartitionMetadata> partitions;

  TopicMetadata(this.errorCode, this.topicName, this.partitions);

  @override
  toString() => 'TopicMetadata: $topicName, errorCode: $errorCode; '
      '$partitions';
}

class PartitionMetadata {
  final int errorCode;
  final int partitionId;
  final int leader;
  final List<int> replicas;
  final List<int> inSyncReplicas;

  PartitionMetadata(this.errorCode, this.partitionId, this.leader,
      this.replicas, this.inSyncReplicas);

  @override
  toString() => 'Partition#$partitionId(errorCode: $errorCode, '
      'leader: $leader, replicas: $replicas, isr: $inSyncReplicas)';
}

class _MetadataRequestV0Encoder implements RequestEncoder<MetadataRequestV0> {
  @override
  List<int> encode(MetadataRequestV0 request) {
    var builder = new KafkaBytesBuilder();
    List<String> topics = request.topicNames ?? new List();
    builder.addStringArray(topics);
    return builder.takeBytes();
  }
}

class _MetadataResponseV0Decoder
    implements ResponseDecoder<MetadataResponseV0> {
  @override
  MetadataResponseV0 decode(List<int> data) {
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
    return new MetadataResponseV0(brokers, topics);
  }
}
