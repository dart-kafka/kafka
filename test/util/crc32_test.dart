import 'package:test/test.dart';
import 'package:kafka/src/util/crc32.dart';

void main() {
  group('Crc32:', () {
    test('it produces valid CRC32 checksums (unsigned)', () {
      _dataProvider().forEach((input, expected) {
        var result = Crc32.unsigned(input);
        expect(result, equals(expected));
      });
    });

    test('it produces valid CRC32 checksums for string inputs', () {
      _stringDataProvider().forEach((input, expected) {
        var result = Crc32.unsigned(input);
        expect(result, equals(expected));
      });
    });

    test('it can produce signed checksum', () {
      var result = Crc32.signed('Lammert'.codeUnits);
      expect(result, equals(0x71FC2734));
    });
  });
}

/// Test cases generated using: http://www.lammertbies.nl/comm/info/crc-calculation.html
Map<List<int>, int> _dataProvider() {
  return {
    [0]: 0xD202EF8D,
    [1]: 0xA505DF1B,
    [113, 38, 83, 70]: 0xC02EC885
  };
}

/// Test cases generated using: http://www.lammertbies.nl/comm/info/crc-calculation.html
Map<List<int>, int> _stringDataProvider() {
  return {
    'Lammert'.codeUnits: 0x71FC2734,
  };
}
