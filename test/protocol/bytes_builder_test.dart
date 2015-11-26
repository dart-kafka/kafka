library kafka.protocol.test.bytes_builder;

import 'dart:async';
import 'dart:convert';
import 'package:test/test.dart';
import 'package:kafka/protocol.dart';

void main() {
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
      var value = UTF8.decode(encodedString);
      expect(value, equals('dart-kafka'));
    });

    test('it adds array values of Int8', () {
      _builder.addArray([34, 45, 12], KafkaType.int8);
      var result = _builder.toBytes();
      expect(result, hasLength(equals(7))); // 4 bytes = size, 3 bytes = values
    });

    test('it adds array values of Int16', () {
      _builder.addArray([234, 523, 332], KafkaType.int16);
      var result = _builder.toBytes();
      expect(result, hasLength(equals(10))); // 4 bytes = size, 6 bytes = values
    });

    test('it adds array values of Int64', () {
      _builder.addArray([234, 523, 332], KafkaType.int64);
      var result = _builder.toBytes();
      expect(
          result, hasLength(equals(28))); // 4 bytes = size, 24 bytes = values
    });

    test('it adds array values of bytes', () {
      _builder.addArray([
        [123],
        [32]
      ], KafkaType.bytes);
      var result = _builder.toBytes();
      expect(result, hasLength(equals(14))); // 4 + 4 + 1 + 4 + 1
    });

    test('it does not support objects in array values', () {
      expect(
          new Future(() {
            _builder.addArray(['foo'], KafkaType.object);
          }),
          throwsStateError);
    });

    test('it supports null for bytes type', () {
      _builder.addBytes(null);
      var result = _builder.toBytes();
      expect(result, hasLength(4));
      expect(result, equals([255, 255, 255, 255]));
    });
  });
}
