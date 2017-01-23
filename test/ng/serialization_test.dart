import 'package:test/test.dart';
import 'package:kafka/ng.dart';

void main() {
  group('Serialization', () {
    test('foo', () {
      var o = new StateDeserializer<Foo>();
      o.deserialize([]);
    });
  });
}

class Foo {}
