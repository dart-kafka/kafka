import 'package:test/test.dart';
import 'package:kafka/src/versions.dart';

void main() {
  group('Versions', () {
    test('fail if there is no overlap with server', () async {
      var server = [new ApiVersion(0, 3, 5)];
      var client = [new ApiVersion(0, 2, 2)];
      expect(overlap(0, 0, 1, 1), isFalse);
      expect(() {
        resolveApiVersions(server, client);
      }, throwsUnsupportedError);
    });

    test('pick max client version if server max is higher', () {
      var server = [new ApiVersion(0, 0, 5)];
      var client = [new ApiVersion(0, 0, 2)];
      var result = resolveApiVersions(server, client);
      expect(result[0], 2);
    });

    test('pick max server version if client max is higher', () {
      var server = [new ApiVersion(0, 0, 1)];
      var client = [new ApiVersion(0, 0, 3)];
      var result = resolveApiVersions(server, client);
      expect(result[0], 1);
    });
  });
}
