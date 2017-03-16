import 'package:kafka/kafka.dart';
import 'package:test/test.dart';

void main() {
  group('MessageAttributes:', () {
    test('get compression from byte', () {
      expect(Compression.none, new MessageAttributes.fromByte(0).compression);
      expect(Compression.gzip, new MessageAttributes.fromByte(1).compression);
      expect(Compression.snappy, new MessageAttributes.fromByte(2).compression);
    });

    test('get timestamp type from byte', () {
      expect(TimestampType.createTime,
          new MessageAttributes.fromByte(0).timestampType);
      expect(TimestampType.logAppendTime,
          new MessageAttributes.fromByte(8).timestampType);
    });
  });
}
