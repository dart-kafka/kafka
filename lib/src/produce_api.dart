import 'common.dart';
import 'errors.dart';
import 'io.dart';
import 'messages.dart';
import 'util/crc32.dart';

class ProduceRequest extends KRequest<ProduceResponse> {
  @override
  final int apiKey = ApiKey.produce;

  /// Indicates how many acknowledgements the servers
  /// should receive before responding to the request.
  final int requiredAcks;

  /// Provides a maximum time in milliseconds the server
  /// can await the receipt of the number of acknowledgements in [requiredAcks].
  final int timeout;

  /// Collection of messages to produce.
  final Map<String, Map<int, List<Message>>> messages;

  ProduceRequest(this.requiredAcks, this.timeout, this.messages);

  @override
  ResponseDecoder<ProduceResponse> get decoder =>
      const _ProduceResponseDecoder();

  @override
  RequestEncoder<KRequest> get encoder => const _ProduceRequestEncoder();
}

class ProduceResponse {
  /// List of produce results for each topic-partition.
  final PartitionResults results;
  final int throttleTime;

  ProduceResponse(this.results, this.throttleTime) {
    var errorResult = results.partitions
        .firstWhere((_) => _.error != Errors.NoError, orElse: () => null);

    if (errorResult is PartitionResult) {
      throw KafkaError.fromCode(errorResult.error, this);
    }
  }

  @override
  String toString() => 'ProduceResponse{$results}';
}

class PartitionResults {
  final List<PartitionResult> partitions;

  PartitionResults(this.partitions);

  Map<TopicPartition, PartitionResult> _asMap;
  Map<TopicPartition, PartitionResult> get asMap {
    if (_asMap != null) return _asMap;
    _asMap = Map.fromIterable(partitions, key: (result) => result.partition);
    return _asMap;
  }

  PartitionResult operator [](TopicPartition partition) => asMap[partition];

  Map<TopicPartition, int> _offsets;
  Map<TopicPartition, int> get offsets {
    if (_offsets != null) return _offsets;
    _offsets = Map.fromIterable(partitions,
        key: (result) => result.partition, value: (result) => result.offset);
    return _offsets;
  }

  int get length => partitions.length;
}

/// Data structure representing result of producing messages with
/// [ProduceRequest].
class PartitionResult {
  /// The topic-parition of this result.
  final TopicPartition partition;

  /// Error code returned by the server.
  final int error;

  /// Offset of the first message.
  final int offset;

  /// The creation timestamp of the message set.
  ///
  /// If `LogAppendTime` is used for the topic this is the timestamp assigned
  /// by the broker to the message set. All the messages in the message set
  /// have the same timestamp.
  ///
  /// If `CreateTime` is used, this field is always -1. The producer can assume
  /// the timestamp of the messages in the produce request has been accepted
  /// by the broker if there is no error code returned.
  final int timestamp;

  PartitionResult(this.partition, this.error, this.offset, this.timestamp);

  String get topic => partition.topic;

  @override
  String toString() =>
      'PartitionResult{${partition}, error: ${error}, offset: ${offset}, timestamp: $timestamp}';
}

class _ProduceRequestEncoder implements RequestEncoder<ProduceRequest> {
  const _ProduceRequestEncoder();

  @override
  List<int> encode(ProduceRequest request, int version) {
    assert(
        version == 2, 'Only v2 of Produce request is supported by the client.');

    var builder = KafkaBytesBuilder();
    builder.addInt16(request.requiredAcks);
    builder.addInt32(request.timeout);

    builder.addInt32(request.messages.length);
    request.messages.forEach((topic, partitions) {
      builder.addString(topic);
      builder.addInt32(partitions.length);
      partitions.forEach((partition, messages) {
        builder.addInt32(partition);
        builder.addRaw(_messageSetToBytes(messages));
      });
    });

    return builder.takeBytes();
  }

  List<int> _messageSetToBytes(List<Message> messages) {
    var builder = KafkaBytesBuilder();
    messages.asMap().forEach((offset, message) {
      var messageData = _messageToBytes(message);
      builder.addInt64(offset);
      builder.addInt32(messageData.length);
      builder.addRaw(messageData);
    });
    var messageData = builder.takeBytes();
    builder.addInt32(messageData.length);
    builder.addRaw(messageData);
    return builder.takeBytes();
  }

  List<int> _messageToBytes(Message message) {
    var builder = KafkaBytesBuilder();
    builder.addInt8(1); // magicByte
    builder.addInt8(_encodeAttributes(message.attributes));
    builder.addInt64(message.timestamp);
    builder.addBytes(message.key);
    builder.addBytes(message.value);

    var data = builder.takeBytes();
    int crc = Crc32.signed(data);
    builder.addInt32(crc);
    builder.addRaw(data);

    return builder.takeBytes();
  }

  int _encodeAttributes(MessageAttributes attributes) {
    switch (attributes.compression) {
      case Compression.none:
        return 0;
      case Compression.gzip:
        return 1;
      case Compression.snappy:
        return 2;
      default:
        throw ArgumentError(
            'Invalid compression value ${attributes.compression}.');
    }
  }
}

class _ProduceResponseDecoder implements ResponseDecoder<ProduceResponse> {
  const _ProduceResponseDecoder();

  @override
  ProduceResponse decode(List<int> data) {
    var reader = KafkaBytesReader.fromBytes(data);
    var results = List<PartitionResult>();
    var topicCount = reader.readInt32();
    while (topicCount > 0) {
      var topic = reader.readString();
      var partitionCount = reader.readInt32();
      while (partitionCount > 0) {
        var partition = reader.readInt32();
        var error = reader.readInt16();
        var offset = reader.readInt64();
        var timestamp = reader.readInt64();
        results.add(PartitionResult(
            TopicPartition(topic, partition), error, offset, timestamp));
        partitionCount--;
      }
      topicCount--;
    }
    var throttleTime = reader.readInt32();
    return ProduceResponse(PartitionResults(results), throttleTime);
  }
}
