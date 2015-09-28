library kafka.test.bytes_builder;

import 'dart:convert';
import 'package:test/test.dart';
import 'package:kafka/kafka.dart';

KafkaBytesBuilder _builder;

void main() {
  setUp(() {
    _builder = new KafkaBytesBuilder();
  });

  test('it adds Int8 values', () {
    _builder.addInt8(35);
    List<int> result = _builder.toBytes();
    expect(result, hasLength(equals(1)));
    expect(result[0], equals(35));
  });

  test('it adds Int16 values', () {
    _builder.addInt16(341);
    List<int> result = _builder.toBytes();
    expect(result, hasLength(equals(2)));
    expect(result, equals([1, 85]));
  });

  test('it adds Int32 values', () {
    _builder.addInt32(1635765);
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

  test('it adds array values', () {
    _builder.addArray([34, 45, 12], KafkaType.int8);
    var result = _builder.toBytes();
    expect(result, hasLength(equals(7))); // 4 bytes = size, 3 bytes = values
  });
}
