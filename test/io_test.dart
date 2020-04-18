import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';

import 'package:kafka/src/io.dart';
import 'package:test/test.dart';

void main() {
  group('PacketStreamTransformer: ', () {
    var _controller = new StreamController<List<int>>();
    test('it transforms incoming byte stream into stream of Kafka packets',
        () async {
      var t = new PacketStreamTransformer();
      var packetStream = _controller.stream.transform(t);
      _controller.add(_createPacket([1, 2, 3, 4]));
      _controller.add(_createPacket([5, 6, 7]));
      _controller.close();

      var result = await packetStream.toList();
      expect(result.length, 2);
      expect(result.first, [1, 2, 3, 4]);
      expect(result.last, [5, 6, 7]);
    });
  });

  group('BytesBuilder:', () {
    KafkaBytesBuilder _builder;

    setUp(() {
      _builder = new KafkaBytesBuilder();
    });

    test('it adds Int8 values', () {
      _builder.addInt8(35);
      expect(_builder.length, equals(1));
      List<int> result = _builder.toBytes();
      expect(result, hasLength(equals(1)));
      expect(result[0], equals(35));
    });

    test('it adds Int16 values', () {
      _builder.addInt16(341);
      expect(_builder.length, equals(2));
      List<int> result = _builder.toBytes();
      expect(result, hasLength(equals(2)));
      expect(result, equals([1, 85]));
    });

    test('it adds Int32 values', () {
      _builder.addInt32(1635765);
      expect(_builder.length, equals(4));
      var result = _builder.toBytes();
      expect(result, hasLength(equals(4)));
      expect(result, equals([0, 24, 245, 181]));
    });

    test('it adds string values', () {
      _builder.addString('dart-kafka');
      var result = _builder.toBytes();
      expect(result, hasLength(equals(12))); // 2 bytes = size, 10 bytes = value
      var encodedString = result.getRange(2, 12).toList();
      var value = utf8.decode(encodedString);
      expect(value, equals('dart-kafka'));
    });

    test('it adds array values of Int8', () {
      _builder.addInt8Array([34, 45, 12]);
      var result = _builder.toBytes();
      expect(result, hasLength(equals(7))); // 4 bytes = size, 3 bytes = data
    });

    test('it adds array values of Int16', () {
      _builder.addInt16Array([234, 523, 332]);
      var result = _builder.toBytes();
      expect(result, hasLength(equals(10))); // 4 bytes = size, 6 bytes = data
    });

    test('it adds array of Int32', () {
      _builder.addInt32Array([234, 523, 332]);
      var result = _builder.toBytes();
      expect(result, hasLength(equals(16))); // 4 bytes = size, 12 bytes = data
    });

    test('it adds array values of Int64', () {
      _builder.addInt64Array([234, 523, 332]);
      var result = _builder.toBytes();
      expect(result, hasLength(equals(28))); // 4 bytes = size, 24 bytes = data
    });

    test('it adds array values of bytes', () {
      _builder.addBytesArray([
        [123],
        [32]
      ]);
      var result = _builder.toBytes();
      expect(result, hasLength(equals(14))); // 4 + 4 + 1 + 4 + 1
    });

    test('it supports null for bytes type', () {
      _builder.addBytes(null);
      var result = _builder.toBytes();
      expect(result, hasLength(4));
      expect(result, equals([255, 255, 255, 255]));
    });
  });

  group('BytesReader:', () {
    KafkaBytesReader _reader;
    List<int> _data;

    setUp(() {
      var builder = new KafkaBytesBuilder();
      builder
        ..addInt8(53)
        ..addInt16(3541)
        ..addInt32(162534612)
        ..addString('dart-kafka')
        ..addBytes([12, 43, 83])
        ..addStringArray(['one', 'two']);
      _data = builder.takeBytes();
      _reader = new KafkaBytesReader.fromBytes(_data);
    });

    test('it indicates end of buffer', () {
      var builder = new KafkaBytesBuilder();
      builder.addInt8(53);
      _reader = new KafkaBytesReader.fromBytes(builder.takeBytes());
      expect(_reader.length, equals(1));
      expect(_reader.isEOF, isFalse);
      expect(_reader.isNotEOF, isTrue);
      _reader.readInt8();
      expect(_reader.isEOF, isTrue);
      expect(_reader.isNotEOF, isFalse);
    });

    test('it reads all Kafka types', () {
      expect(_reader.readInt8(), equals(53));
      expect(_reader.readInt16(), equals(3541));
      expect(_reader.readInt32(), equals(162534612));
      expect(_reader.readString(), equals('dart-kafka'));
      expect(_reader.readBytes(), equals([12, 43, 83]));
      expect(_reader.readStringArray(), equals(['one', 'two']));
    });

    test('it supports null for bytes type', () {
      var builder = new KafkaBytesBuilder();
      builder.addBytes(null);
      var reader = new KafkaBytesReader.fromBytes(builder.takeBytes());
      expect(reader.readBytes(), equals(null));
    });
  });
}

List<int> _createPacket(List<int> payload) {
  ByteData bdata = new ByteData(4);
  bdata.setInt32(0, payload.length);
  return bdata.buffer.asInt8List().toList()..addAll(payload);
}
