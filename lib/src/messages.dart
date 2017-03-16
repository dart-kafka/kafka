import 'dart:collection';

/// Compression types supported by Kafka.
enum Compression { none, gzip, snappy }

/// Types of [Message]'s timestamp supported by Kafka.
enum TimestampType { createTime, logAppendTime }

const int _compressionMask = 0x07;

const Map _kIntToCompression = const {
  0: Compression.none,
  1: Compression.gzip,
  2: Compression.snappy,
};

const Map _kIntToTimestamptype = const {
  0: TimestampType.createTime,
  1: TimestampType.logAppendTime,
};

/// Kafka Message Attributes.
class MessageAttributes {
  /// Compression codec.
  final Compression compression;

  /// The type of this message's timestamp.
  final TimestampType timestampType;

  /// Creates new instance of MessageAttributes.
  MessageAttributes(
      {this.compression = Compression.none,
      this.timestampType = TimestampType.createTime});

  /// Creates MessageAttributes from the raw byte.
  MessageAttributes.fromByte(int byte)
      : compression = _kIntToCompression[byte & _compressionMask],
        timestampType = _kIntToTimestamptype[(byte >> 3) & 1];

  @override
  String toString() => '{compression: $compression, tsType: $timestampType}';
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

  /// The timestamp of this message, in msecs.
  final int timestamp;

  /// Default internal constructor.
  Message._(this.attributes, this.key, this.value, this.timestamp);

  /// Creates new [Message].
  factory Message(List<int> value,
      {MessageAttributes attributes, List<int> key, int timestamp}) {
    attributes ??= new MessageAttributes();
    timestamp ??= new DateTime.now().millisecondsSinceEpoch;
    return new Message._(attributes, key, value, timestamp);
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
//  factory MessageSet.build(envelope) {
//    if (envelope.compression == KafkaCompression.none) {
//      return new MessageSet(envelope.messages.asMap());
//    } else {
//      if (envelope.compression == KafkaCompression.snappy)
//        throw new ArgumentError(
//            'Snappy compression is not supported yet by the client.');
//
//      // var codec = new GZipCodec();
//      // var innerEnvelope = new ProduceEnvelope(
//      //     envelope.topicName, envelope.partitionId, envelope.messages);
//      // var innerMessageSet = new MessageSet.build(innerEnvelope);
//      // var value = codec.encode(innerMessageSet.toBytes());
//      // var attrs = new MessageAttributes(KafkaCompression.gzip);
//
//      // return new MessageSet({0: new Message(value, attributes: attrs)});
//    }
//  }
}
