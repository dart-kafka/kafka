part of kafka.protocol;

/// Provides convenience methods read Kafka specific data types from a stream of bytes.
class KafkaBytesReader {
  Int8List _data;
  int _offset = 0;

  /// Current position in this buffer.
  int get offset => _offset;

  /// Size of this byte buffer.
  int get length => _data.length;

  /// Whether this bytes buffer has been fully read.
  bool get isEOF => _data.length == _offset;

  /// Whether there are still unread bytes left in this buffer.
  bool get isNotEOF => !isEOF;

  /// Creates reader from a list of bytes.
  KafkaBytesReader.fromBytes(List<int> data) {
    this._data = new Int8List.fromList(data);
  }

  // Reads int8 from the data and returns it.
  int readInt8() {
    var data = new ByteData.view(_data.buffer, _offset, 1);
    var value = data.getInt8(0);
    _offset += 1;

    return value;
  }

  /// Reads 16-bit integer from the current position of this buffer.
  int readInt16() {
    var data = new ByteData.view(_data.buffer, _offset, 2);
    var value = data.getInt16(0);
    _offset += 2;

    return value;
  }

  /// Reads 32-bit integer from the current position of this buffer.
  int readInt32() {
    var data = new ByteData.view(_data.buffer, _offset, 4);
    var value = data.getInt32(0);
    _offset += 4;

    return value;
  }

  /// Reads 64-bit integer from the current position of this buffer.
  int readInt64() {
    var data = new ByteData.view(_data.buffer, _offset, 8);
    var value = data.getInt64(0);
    _offset += 8;

    return value;
  }

  String readString() {
    var length = readInt16();
    var value = _data.buffer.asInt8List(_offset, length).toList();
    var valueAsString = UTF8.decode(value);
    _offset += length;

    return valueAsString;
  }

  List<int> readBytes() {
    var length = readInt32();
    if (length == -1) {
      return null;
    } else {
      var value = _data.buffer.asInt8List(_offset, length).toList();
      _offset += length;
      return value;
    }
  }

  List readArray(KafkaType itemType,
      [dynamic objectReadHandler(KafkaBytesReader reader)]) {
    var length = readInt32();
    var items = new List();
    for (var i = 0; i < length; i++) {
      switch (itemType) {
        case KafkaType.int8:
          items.add(readInt8());
          break;
        case KafkaType.int16:
          items.add(readInt16());
          break;
        case KafkaType.int32:
          items.add(readInt32());
          break;
        case KafkaType.int64:
          items.add(readInt64());
          break;
        case KafkaType.string:
          items.add(readString());
          break;
        case KafkaType.bytes:
          items.add(readBytes());
          break;
        case KafkaType.object:
          if (objectReadHandler == null) {
            throw new StateError('ObjectReadHandler must be provided');
          }
          items.add(objectReadHandler(this));
          break;
      }
    }

    return items;
  }

  /// Reads raw bytes from this buffer.
  List<int> readRaw(int length) {
    var value = _data.buffer.asInt8List(_offset, length).toList();
    _offset += length;

    return value;
  }
}
