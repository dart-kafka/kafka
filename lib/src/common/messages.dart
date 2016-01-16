part of kafka.common;

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

  /// Converts this attributes into byte.
  int toInt() {
    return _compressionToInt();
  }

  int _compressionToInt() {
    switch (this.compression) {
      case KafkaCompression.none:
        return 0;
      case KafkaCompression.gzip:
        return 1;
      case KafkaCompression.snappy:
        return 2;
    }
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

/// Envelope used for publishing messages to Kafka.
class ProduceEnvelope {
  /// Name of the topic.
  final String topicName;

  /// Partition ID.
  final int partitionId;

  /// List of messages to publish.
  final List<Message> messages;

  /// Compression codec to be used.
  final KafkaCompression compression;

  /// Creates new envelope containing list of messages.
  ///
  /// You can optionally set [compression] codec which will be used to encode
  /// messages.
  ProduceEnvelope(this.topicName, this.partitionId, this.messages,
      {this.compression: KafkaCompression.none}) {
    messages.forEach((m) {
      if (m.attributes.compression != KafkaCompression.none) {
        throw new StateError(
            'ProduceEnvelope: compression can not be set on individual messages in ProduceEnvelope, use ProduceEnvelope.compression instead.');
      }
    });
  }
}
