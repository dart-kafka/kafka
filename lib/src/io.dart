import 'dart:async';
import 'dart:convert';
import 'dart:ffi';
import 'dart:io';
import 'dart:typed_data';

import 'package:logging/logging.dart';

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
    addString('dart_kafka-0.10');
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
    List<int> data = utf8.encode(value);
    addInt16(data.length);
    _builder.add(data);
  }

  void _addArray<T>(List<T> items, addFunc(T item)) {
    addInt32(items.length);
    items.forEach((_) {
      addFunc(_);
    });
  }

  void addInt8Array(List<int> items) {
    _addArray<int>(items, addInt8);
  }

  void addInt16Array(List<int> items) {
    _addArray<int>(items, addInt16);
  }

  void addInt32Array(List<int> items) {
    _addArray<int>(items, addInt32);
  }

  void addInt64Array(List<int> items) {
    _addArray<int>(items, addInt64);
  }

  void addStringArray(List<String> items) {
    _addArray<String>(items, addString);
  }

  void addBytesArray(List<List<int>> items) {
    _addArray<List<int>>(items, addBytes);
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

/// Provides convenience methods to read Kafka specific data types from a
/// stream of bytes.
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
    var valueAsString = utf8.decode(value);
    _offset += length;

    return valueAsString;
  }

  T readObject<T>(T readFunc(KafkaBytesReader reader)) => readFunc(this);

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

  List<int> readInt8Array() => _readArray(readInt8);
  List<int> readInt16Array() => _readArray(readInt16);
  List<int> readInt32Array() => _readArray(readInt32);
  List<int> readInt64Array() => _readArray(readInt64);
  List<String> readStringArray() => _readArray(readString);
  List<List<int>> readBytesArray() => _readArray(readBytes);
  List<T> readObjectArray<T>(T readFunc(KafkaBytesReader reader)) {
    return _readArray<T>(() => readFunc(this));
  }

  List<T> _readArray<T>(T reader()) {
    var length = readInt32();
    var items = new List<T>();
    for (var i = 0; i < length; i++) {
      items.add(reader());
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

class PacketStreamTransformer
    implements StreamTransformer<Uint8List, List<int>> {
  List<int> _data = new List<int>();
  StreamController<List<int>> _controller = new StreamController<List<int>>();

  @override
  Stream<List<int>> bind(Stream<List<int>> stream) {
    stream.listen(_onData, onError: _onError, onDone: _onDone);
    return _controller.stream;
  }

  void _onData(List<int> data) {
    _data.addAll(data);

    while (true) {
      if (_data.length >= 4) {
        var sizeBytes = new Int8List.fromList(_data.sublist(0, 4));
        var bd = new ByteData.view(sizeBytes.buffer);
        var size = bd.getInt32(0);
        if (_data.length >= size + 4) {
          List<int> packetData = _data.sublist(4, size + 4);
          _controller.add(packetData);
          _data.removeRange(0, size + 4);
        } else {
          break; // not enough data
        }
      } else {
        break; // not enough data
      }
    }
  }

  void _onError(error) {
    _controller.addError(error);
  }

  void _onDone() {
    _controller.close();
  }

  @override
  StreamTransformer<RS, RT> cast<RS, RT>() => StreamTransformer.castFrom(this);
}

/// Handles Kafka channel multiplexing.
// TODO: Consider using RawSocket internally for more efficiency.
class KSocket {
  static Logger _logger = new Logger('KSocket');

  final Socket _ioSocket;
  Stream<List<int>> _stream;
  StreamSubscription<List<int>> _subscription;

  int _nextCorrelationId = 1;
  int get nextCorrelationId => _nextCorrelationId++;

  Map<int, Completer<List<int>>> _inflightRequests = new Map();

  KSocket._(this._ioSocket) {
    _ioSocket.setOption(SocketOption.tcpNoDelay, true);
    _stream = _ioSocket.transform(PacketStreamTransformer());
    _subscription = _stream.listen(_onPacket);
  }

  static Future<KSocket> connect(String host, int port) {
    return Socket.connect(host, port).then((socket) => new KSocket._(socket));
  }

  Future _flushFuture = new Future.value();

  Future<List<int>> sendPacket(int apiKey, int apiVersion, List<int> payload) {
    var correlationId = nextCorrelationId;
    var completer = new Completer<List<int>>();
    _inflightRequests[correlationId] = completer;

    var bb = new KafkaBytesBuilder.withRequestHeader(
        apiKey, apiVersion, correlationId);
    var header = bb.takeBytes();
    bb.addInt32(header.length + payload.length);
    var length = bb.takeBytes();

    _flushFuture = _flushFuture.then((_) {
      _ioSocket..add(length)..add(header)..add(payload);
      return _ioSocket.flush();
    }).catchError((error) {
      completer.completeError(error);
      _inflightRequests.remove(correlationId);
    });
    return completer.future;
  }

  void _onPacket(List<int> packet) {
    var r = new KafkaBytesReader.fromBytes(packet.sublist(0, 4));
    var correlationId = r.readInt32();
    var completer = _inflightRequests[correlationId];
    if (completer.isCompleted) {
      _logger.warning('Received packet for already completed request.');
    } else {
      packet.removeRange(0, 4); // removes correlationId from the payload
      completer.complete(packet);
    }
    _inflightRequests.remove(correlationId);
  }

  Future destroy() async {
    await _subscription.cancel();
    _ioSocket.destroy();
  }
}

abstract class RequestEncoder<T extends KRequest> {
  List<int> encode(T request, int version);
}

abstract class ResponseDecoder<T> {
  T decode(List<int> data);
}

abstract class KRequest<T> {
  /// Unique numeric key of this API request.
  int get apiKey;

  RequestEncoder<KRequest> get encoder;
  ResponseDecoder<T> get decoder;
}
