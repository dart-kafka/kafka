import '../util/crc32.dart';
import 'errors.dart';
import 'io.dart';
import 'messages.dart';

class ProduceRequestV2 extends KRequest<ProduceResponseV2> {
  @override
  final int apiKey = 0;

  @override
  final int apiVersion = 2;

  /// Indicates how many acknowledgements the servers
  /// should receive before responding to the request.
  final int requiredAcks;

  /// Provides a maximum time in milliseconds the server
  /// can await the receipt of the number of acknowledgements in [requiredAcks].
  final int timeout;

  /// Collection of messages to produce.
  final Map<String, Map<int, List<Message>>> messages;

  ProduceRequestV2(this.requiredAcks, this.timeout, this.messages);

  @override
  ResponseDecoder<ProduceResponseV2> get decoder =>
      new _ProduceResponseV2Decoder();

  @override
  RequestEncoder<KRequest> get encoder => new _ProduceRequestV2Encoder();
}

class ProduceResponseV2 {
  /// List of produce results for each topic-partition.
  final List<TopicProduceResult> results;
  final int throttleTime;

  ProduceResponseV2(this.results, this.throttleTime) {
    var errorResult = results.firstWhere(
        (_) => _.errorCode != KafkaServerError.NoError_,
        orElse: () => null);

    if (errorResult is TopicProduceResult) {
      throw new KafkaServerError.fromCode(errorResult.errorCode, this);
    }
  }
}

/// Data structure representing result of producing messages with
/// [ProduceRequestV2].
class TopicProduceResult {
  /// The name of the topic.
  final String topic;

  /// The ID of the partition.
  final int partition;

  /// Error code returned by the server.
  final int errorCode;

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

  TopicProduceResult(
      this.topic, this.partition, this.errorCode, this.offset, this.timestamp);

  @override
  String toString() =>
      'ProduceResult{${topic}:${partition}, error: ${errorCode}, offset: ${offset}, timestamp: $timestamp}';
}

class _ProduceRequestV2Encoder implements RequestEncoder<ProduceRequestV2> {
  @override
  List<int> encode(ProduceRequestV2 request) {
    var builder = new KafkaBytesBuilder();
    builder.addInt16(request.requiredAcks);
    builder.addInt32(request.timeout);

    builder.addInt32(request.messages.length);
    request.messages.forEach((topic, partitions) {
      builder.addString(topic);
      builder.addInt32(partitions.length);
      partitions.forEach((partitionId, messages) {
        builder.addInt32(partitionId);
        builder.addRaw(_messageSetToBytes(messages));
      });
    });

    return builder.takeBytes();
  }

  List<int> _messageSetToBytes(List<Message> messages) {
    var builder = new KafkaBytesBuilder();
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
    var builder = new KafkaBytesBuilder();
    builder.addInt8(1); // magicByte
    builder.addInt8(_encodeAttributes(message.attributes));
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
        throw new ArgumentError(
            'Invalid compression value ${attributes.compression}.');
    }
  }
}

class _ProduceResponseV2Decoder extends ResponseDecoder<ProduceResponseV2> {
  @override
  ProduceResponseV2 decode(List<int> data) {
    var reader = new KafkaBytesReader.fromBytes(data);
    var results = new List<TopicProduceResult>();
    var topicCount = reader.readInt32();
    while (topicCount > 0) {
      var topicName = reader.readString();
      var partitionCount = reader.readInt32();
      while (partitionCount > 0) {
        var partitionId = reader.readInt32();
        var errorCode = reader.readInt16();
        var offset = reader.readInt64();
        var timestamp = reader.readInt64();
        results.add(new TopicProduceResult(
            topicName, partitionId, errorCode, offset, timestamp));
        partitionCount--;
      }
      topicCount--;
    }
    var throttleTime = reader.readInt32();
    return new ProduceResponseV2(results, throttleTime);
  }
}
