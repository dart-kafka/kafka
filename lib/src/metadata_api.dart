import 'common.dart';
import 'errors.dart';
import 'io.dart';

/// Kafka MetadataRequest.
class MetadataRequest extends KRequest<MetadataResponse> {
  @override
  final int apiKey = ApiKey.metadata;

  @override
  final RequestEncoder<KRequest> encoder = const _MetadataRequestEncoder();

  @override
  final ResponseDecoder<MetadataResponse> decoder =
      const _MetadataResponseDecoder();

  final List<String> topics;

  /// Creates MetadataRequest.
  ///
  /// If [topics] is not set it fetches information about all existing
  /// topics in the Kafka cluster.
  MetadataRequest([this.topics]);
}

class MetadataResponse {
  /// List of brokers in a Kafka cluster.
  final List<Broker> brokers;

  /// List of topics in a Kafka cluster.
  final Topics topics;

  MetadataResponse(this.brokers, this.topics) {
    var errorTopic = topics._topics
        .firstWhere((_) => _.error != Errors.NoError, orElse: () => null);
    // TODO: also loop through partitions to find errors on a partition level.
    if (errorTopic is Topic) {
      throw KafkaError.fromCode(errorTopic.error, this);
    }
  }
}

/// Represents a set of Kafka topics.
class Topics {
  final List<Topic> _topics;

  /// List of Kafka brokers.
  final Brokers brokers;

  Topics(this._topics, this.brokers);

  Topic operator [](String topic) => asMap[topic];

  List<Topic> get asList => List.unmodifiable(_topics);

  Map<String, Topic> _asMap;

  /// Returns a map where keys are topic names.
  Map<String, Topic> get asMap {
    if (_asMap != null) return _asMap;
    var map = Map.fromIterable(
      _topics,
      key: (topic) => topic.name,
    );
    _asMap = Map.unmodifiable(map);
    return _asMap;
  }

  /// The list of topic names.
  List<String> get names {
    return asMap.keys.toList(growable: false);
  }

  /// The size of this topics set.
  int get length => _topics.length;

  List<TopicPartition> _topicPartitions;

  /// List of topic-partitions accross all topics in this set.
  List<TopicPartition> get topicPartitions {
    if (_topicPartitions != null) return _topicPartitions;
    _topicPartitions = _topics.expand<TopicPartition>((topic) {
      return topic.partitions._partitions
          .map((partition) => TopicPartition(topic.name, partition.id));
    }).toList(growable: false);
    return _topicPartitions;
  }
}

/// Represents a list of Kafka brokers.
class Brokers {
  final List<Broker> _brokers;

  Brokers(this._brokers);

  Broker operator [](int id) => asMap[id];

  Map<int, Broker> _asMap;
  Map<int, Broker> get asMap {
    if (_asMap != null) return _asMap;
    var map = Map.fromIterable(_brokers, key: (broker) => broker.id);
    _asMap = Map.unmodifiable(map);
    return _asMap;
  }
}

class Topic {
  final int error;
  final String name;
  final Partitions partitions;

  Topic(this.error, this.name, this.partitions);

  @override
  toString() => 'Topic{$name, error: $error; $partitions}';
}

class Partitions {
  final List<Partition> _partitions;

  Partitions(this._partitions);

  Partition operator [](int id) => asMap[id];

  Map<int, Partition> _asMap;
  Map<int, Partition> get asMap {
    if (_asMap != null) return _asMap;
    _asMap = Map.fromIterable(_partitions, key: (partition) => partition.id);
    return _asMap;
  }

  /// Number of partitions.
  int get length => _partitions.length;
}

class Partition {
  final int error;
  final int id;
  final int leader;
  final List<int> replicas;
  final List<int> inSyncReplicas;

  Partition(
      this.error, this.id, this.leader, this.replicas, this.inSyncReplicas);

  @override
  toString() => 'Partition#$id{error: $error, '
      'leader: $leader, replicas: $replicas, isr: $inSyncReplicas}';
}

class _MetadataRequestEncoder implements RequestEncoder<MetadataRequest> {
  const _MetadataRequestEncoder();

  @override
  List<int> encode(MetadataRequest request, int version) {
    assert(version == 0,
        'Only v0 of Metadata request is supported by the client, $version given.');
    var builder = KafkaBytesBuilder();
    List<String> topics = request.topics ?? List();
    builder.addStringArray(topics);
    return builder.takeBytes();
  }
}

class _MetadataResponseDecoder implements ResponseDecoder<MetadataResponse> {
  const _MetadataResponseDecoder();

  @override
  MetadataResponse decode(List<int> data) {
    var reader = KafkaBytesReader.fromBytes(data);
    List<Broker> brokers = reader.readObjectArray((r) {
      return Broker(r.readInt32(), r.readString(), r.readInt32());
    });

    var topics = reader.readObjectArray((r) {
      var error = reader.readInt16();
      var topic = reader.readString();

      List<Partition> partitions = reader.readObjectArray((r) => Partition(
          r.readInt16(),
          r.readInt32(),
          r.readInt32(),
          r.readInt32Array(),
          r.readInt32Array()));
      return Topic(error, topic, Partitions(partitions));
    });
    return MetadataResponse(brokers, Topics(topics, Brokers(brokers)));
  }
}
