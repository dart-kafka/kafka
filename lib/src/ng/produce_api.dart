import '../util/crc32.dart';
import 'errors.dart';
import 'io.dart';
import 'messages.dart';

class ProduceRequestV0 extends KRequest<ProduceResponseV0> {
  @override
  final int apiKey = 0;

  @override
  final int apiVersion = 0;

  /// Indicates how many acknowledgements the servers
  /// should receive before responding to the request.
  final int requiredAcks;

  /// Provides a maximum time in milliseconds the server
  /// can await the receipt of the number of acknowledgements in [requiredAcks].
  final int timeout;

  /// Collection of messages to produce.
  final Map<String, Map<int, List<Message>>> messages;

  ProduceRequestV0(this.requiredAcks, this.timeout, this.messages);

  @override
  ResponseDecoder<ProduceResponseV0> get decoder =>
      new _ProduceResponseV0Decoder();

  @override
  RequestEncoder<KRequest> get encoder => new _ProduceRequestV0Encoder();
}

class ProduceResponseV0 {
  /// List of produce results for each topic-partition.
  final List<TopicProduceResult> results;

  ProduceResponseV0(this.results) {
    var errorResult = results.firstWhere(
        (_) => _.errorCode != KafkaServerError.NoError_,
        orElse: () => null);

    if (errorResult is TopicProduceResult) {
      throw new KafkaServerError.fromCode(errorResult.errorCode, this);
    }
  }
}

/// Data structure representing result of producing messages with
/// [ProduceRequest].
class TopicProduceResult {
  /// Name of the topic.
  final String topicName;

  /// ID of the partition.
  final int partitionId;

  /// Error code returned by the server.
  final int errorCode;

  /// Offset of the first message.
  final int offset;

  TopicProduceResult(
      this.topicName, this.partitionId, this.errorCode, this.offset);

  @override
  String toString() =>
      'Topic: ${topicName}, partition: ${partitionId}, errorCode: ${errorCode}, offset: ${offset}';
}

class _ProduceRequestV0Encoder implements RequestEncoder<ProduceRequestV0> {
  @override
  List<int> encode(ProduceRequestV0 request) {
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
    builder.addInt8(0); // magicByte
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
      case KafkaCompression.none:
        return 0;
      case KafkaCompression.gzip:
        return 1;
      case KafkaCompression.snappy:
        return 2;
      default:
        throw new ArgumentError(
            'Invalid compression value ${attributes.compression}.');
    }
  }
}

class _ProduceResponseV0Decoder extends ResponseDecoder<ProduceResponseV0> {
  @override
  ProduceResponseV0 decode(List<int> data) {
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
        results.add(
            new TopicProduceResult(topicName, partitionId, errorCode, offset));
        partitionCount--;
      }
      topicCount--;
    }
    return new ProduceResponseV0(results);
  }
}
