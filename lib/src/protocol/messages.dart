part of kafka.protocol;

/// Kafka MessageSet type.
class MessageSet {
  /// Collection of messages. Keys in the map are message offsets.
  final Map<int, Message> _messages;

  /// Map of message offsets to corresponding messages.
  Map<int, Message> get messages => new UnmodifiableMapView(_messages);

  /// Number of messages in this set.
  int get length => _messages.length;

  MessageSet._(this._messages);

  /// Builds new message set for publishing.
  factory MessageSet.build(ProduceEnvelope envelope) {
    if (envelope.compression == KafkaCompression.none) {
      return new MessageSet._(envelope.messages.asMap());
    } else {
      if (envelope.compression == KafkaCompression.snappy)
        throw new ArgumentError(
            'Snappy compression is not supported yet by the client.');

      var codec = new GZipCodec();
      var innerEnvelope = new ProduceEnvelope(
          envelope.topicName, envelope.partitionId, envelope.messages);
      var innerMessageSet = new MessageSet.build(innerEnvelope);
      var value = codec.encode(innerMessageSet.toBytes());
      var attrs = new MessageAttributes(KafkaCompression.gzip);

      return new MessageSet._({0: new Message(value, attributes: attrs)});
    }
  }

  /// Creates new MessageSet from provided data.
  factory MessageSet.fromBytes(KafkaBytesReader reader) {
    int messageSize = -1;
    var messages = new Map<int, Message>();
    while (reader.isNotEOF) {
      try {
        int offset = reader.readInt64();
        messageSize = reader.readInt32();
        var crc = reader.readInt32();

        var data = reader.readRaw(messageSize - 4);
        var actualCrc = Crc32.signed(data);
        if (actualCrc != crc) {
          kafkaLogger?.warning(
              'Message CRC sum mismatch. Expected crc: ${crc}, actual: ${actualCrc}');
          throw new MessageCrcMismatchError(
              'Expected crc: ${crc}, actual: ${actualCrc}');
        }
        var messageReader = new KafkaBytesReader.fromBytes(data);
        var message = _readMessage(messageReader);
        if (message.attributes.compression == KafkaCompression.none) {
          messages[offset] = message;
        } else {
          if (message.attributes.compression == KafkaCompression.snappy)
            throw new ArgumentError(
                'Snappy compression is not supported yet by the client.');

          var codec = new GZipCodec();
          var innerReader =
              new KafkaBytesReader.fromBytes(codec.decode(message.value));
          var innerMessageSet = new MessageSet.fromBytes(innerReader);
          for (var innerOffset in innerMessageSet.messages.keys) {
            messages[innerOffset] = innerMessageSet.messages[innerOffset];
          }
        }
      } on RangeError {
        // According to spec server is allowed to return partial
        // messages, so we just ignore it here and exit the loop.
        var remaining = reader.length - reader.offset;
        kafkaLogger?.info(
            'Encountered partial message. Expected message size: ${messageSize}, bytes left in buffer: ${remaining}, total buffer size ${reader.length}');
        break;
      }
    }

    return new MessageSet._(messages);
  }

  static Message _readMessage(KafkaBytesReader reader) {
    reader.readInt8(); // magicByte
    var attributes = new MessageAttributes.fromByte(reader.readInt8());
    var key = reader.readBytes();
    var value = reader.readBytes();

    return new Message(value, attributes: attributes, key: key);
  }

  /// Converts this MessageSet into sequence of bytes according to Kafka
  /// protocol.
  List<int> toBytes() {
    var builder = new KafkaBytesBuilder();
    _messages.forEach((offset, message) {
      var messageData = _messageToBytes(message);
      builder.addInt64(offset);
      builder.addInt32(messageData.length);
      builder.addRaw(messageData);
    });

    return builder.toBytes();
  }

  List<int> _messageToBytes(Message message) {
    var builder = new KafkaBytesBuilder();
    builder.addInt8(0); // magicByte
    builder.addInt8(message.attributes.toInt());
    builder.addBytes(message.key);
    builder.addBytes(message.value);

    var data = builder.takeBytes();
    int crc = Crc32.signed(data);
    builder.addInt32(crc);
    builder.addRaw(data);

    return builder.toBytes();
  }
}
