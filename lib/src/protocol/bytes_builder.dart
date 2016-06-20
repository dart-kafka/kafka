part of kafka.protocol;

enum KafkaType { int8, int16, int32, int64, string, bytes, object }

/// Bytes builder specific to Kafka protocol.
///
/// Provides convenient methods for writing all Kafka data types (and some more):
/// int8, int16, int32, string, bytes, array.
class KafkaBytesBuilder {
  BytesBuilder _builder = new BytesBuilder();

  int get length => _builder.length;

  /// Creates new builder with empty buffer.
  KafkaBytesBuilder();

  /// Creates new builder and initializes buffer with proper request header.
  KafkaBytesBuilder.withRequestHeader(
      int apiKey, int apiVersion, int correlationId) {
    addInt16(apiKey);
    addInt16(apiVersion);
    addInt32(correlationId);
    addString(dartKafkaId);
  }

  /// Adds 8 bit integer to this buffer.
  void addInt8(int value) {
    ByteData bdata = new ByteData(1);
    bdata.setInt8(0, value);
    _add(bdata);
  }

  /// Adds 16 bit integer to this buffer.
  void addInt16(int value) {
    ByteData bdata = new ByteData(2);
    bdata.setInt16(0, value);
    _add(bdata);
  }

  /// Adds 32 bit integer to this buffer.
  void addInt32(int value) {
    ByteData bdata = new ByteData(4);
    bdata.setInt32(0, value);
    _add(bdata);
  }

  /// Adds 64 bit integer to this buffer.
  void addInt64(int value) {
    ByteData bdata = new ByteData(8);
    bdata.setInt64(0, value);
    _add(bdata);
  }

  /// Adds Kafka string to this bytes builder.
  ///
  /// Kafka string type starts with int16 indicating size of the string
  /// followed by the actual string value.
  void addString(String value) {
    List<int> data = UTF8.encode(value);
    addInt16(data.length);
    _builder.add(data);
  }

  /// Adds Kafka array to this bytes builder.
  ///
  /// Kafka array starts with int32 indicating size of the array followed by
  /// the array items encoded according to their [KafkaType]
  void addArray(Iterable items, KafkaType itemType) {
    addInt32(items.length);
    for (var item in items) {
      switch (itemType) {
        case KafkaType.int8:
          addInt8(item);
          break;
        case KafkaType.int16:
          addInt16(item);
          break;
        case KafkaType.int32:
          addInt32(item);
          break;
        case KafkaType.int64:
          addInt64(item);
          break;
        case KafkaType.string:
          addString(item);
          break;
        case KafkaType.bytes:
          addBytes(new List<int>.from(item));
          break;
        case KafkaType.object:
          throw new StateError('Objects are not supported yet');
          break;
      }
    }
  }

  /// Adds value of Kafka-specific Bytes type to this builder.
  ///
  /// Kafka Bytes type starts with int32 indicating size of the value following
  /// by actual value bytes.
  void addBytes(List<int> value) {
    if (value == null) {
      addInt32(-1);
    } else {
      addInt32(value.length);
      _builder.add(value);
    }
  }

  /// Adds arbitrary data to this buffer.
  void addRaw(List<int> data) {
    _builder.add(data);
  }

  void _add(ByteData data) {
    _builder.add(data.buffer.asInt8List().toList(growable: false));
  }

  List<int> takeBytes() => _builder.takeBytes();

  List<int> toBytes() => _builder.toBytes();
}
