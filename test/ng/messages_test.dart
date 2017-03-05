import 'package:kafka/ng.dart';
import 'package:test/test.dart';

void main() {
  group('MessageAttributes:', () {
    test('get compression from int', () {
      expect(Compression.none, new MessageAttributes.fromByte(0).compression);
      expect(Compression.gzip, new MessageAttributes.fromByte(1).compression);
      expect(Compression.snappy, new MessageAttributes.fromByte(2).compression);
    });
  });
}
