import 'dart:async';
import 'dart:typed_data';

import 'package:kafka/src/ng/io.dart';
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
}

List<int> _createPacket(List<int> payload) {
  ByteData bdata = new ByteData(4);
  bdata.setInt32(0, payload.length);
  return bdata.buffer.asInt8List().toList()..addAll(payload);
}
