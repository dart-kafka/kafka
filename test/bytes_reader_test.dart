library kafka.test.bytes_reader;

import 'package:test/test.dart';
import 'package:kafka/kafka.dart';

KafkaBytesReader _reader;
List<int> _data;

void main() {
  setUp(() {
    var builder = new KafkaBytesBuilder();
    builder
      ..addInt8(53)
      ..addInt16(3541)
      ..addInt32(162534612)
      ..addString('dart-kafka')
      ..addBytes([12, 43, 83])
      ..addArray(['one', 'two'], KafkaType.string);
    _data = builder.takeBytes();
    _reader = new KafkaBytesReader.fromBytes(_data);
  });

  test('it reads all Kafka types', () {
    expect(_reader.readInt8(), equals(53));
    expect(_reader.readInt16(), equals(3541));
    expect(_reader.readInt32(), equals(162534612));
    expect(_reader.readString(), equals('dart-kafka'));
    expect(_reader.readBytes(), equals([12, 43, 83]));
    expect(_reader.readArray(KafkaType.string), equals(['one', 'two']));
  });
}
