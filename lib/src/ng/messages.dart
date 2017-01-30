import 'dart:collection';
// import 'dart:io';
// import '../util/crc32.dart';

/// Compression types supported by Kafka.
enum KafkaCompression { none, gzip, snappy }

/// Kafka Message Attributes. Only [KafkaCompression] is supported by the
/// server at the moment.
class MessageAttributes {
  /// Compression codec.
  final KafkaCompression compression;

  /// Creates new instance of MessageAttributes.
  MessageAttributes([this.compression = KafkaCompression.none]);

  /// Creates MessageAttributes from the raw byte.
  MessageAttributes.fromByte(int byte) : compression = getCompression(byte);

  static KafkaCompression getCompression(int byte) {
    var c = byte & 3;
    var map = {
      0: KafkaCompression.none,
      1: KafkaCompression.gzip,
      2: KafkaCompression.snappy,
    };
    return map[c];
  }
}

/// Kafka Message as defined in the protocol.
class Message {
  /// Metadata attributes about this message.
  final MessageAttributes attributes;

  /// Actual message contents.
  final List<int> value;

  /// Optional message key that was used for partition assignment.
  /// The key can be `null`.
  final List<int> key;

  /// Default internal constructor.
  Message._(this.attributes, this.key, this.value);

  /// Creates new [Message].
  factory Message(List<int> value,
      {MessageAttributes attributes, List<int> key}) {
    attributes ??= new MessageAttributes();
    return new Message._(attributes, key, value);
  }
}

/// Kafka MessageSet type.
class MessageSet {
  /// Collection of messages. Keys in the map are message offsets.
  final Map<int, Message> _messages;

  /// Map of message offsets to corresponding messages.
  Map<int, Message> get messages => new UnmodifiableMapView(_messages);

  /// Number of messages in this set.
  int get length => _messages.length;

  MessageSet(this._messages);

  /// Builds new message set for publishing.
  factory MessageSet.build(envelope) {
    if (envelope.compression == KafkaCompression.none) {
      return new MessageSet(envelope.messages.asMap());
    } else {
      if (envelope.compression == KafkaCompression.snappy)
        throw new ArgumentError(
            'Snappy compression is not supported yet by the client.');

      // var codec = new GZipCodec();
      // var innerEnvelope = new ProduceEnvelope(
      //     envelope.topicName, envelope.partitionId, envelope.messages);
      // var innerMessageSet = new MessageSet.build(innerEnvelope);
      // var value = codec.encode(innerMessageSet.toBytes());
      // var attrs = new MessageAttributes(KafkaCompression.gzip);

      // return new MessageSet({0: new Message(value, attributes: attrs)});
    }
  }
}
