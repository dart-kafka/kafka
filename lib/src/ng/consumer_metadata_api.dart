import 'io.dart';
import 'errors.dart';

class GroupCoordinatorRequestV0 extends KRequest<GroupCoordinatorResponseV0> {
  @override
  final int apiKey = 10;

  @override
  final int apiVersion = 0;

  GroupCoordinatorRequestV0(this.consumerGroup);

  @override
  final ResponseDecoder<GroupCoordinatorResponseV0> decoder =
      new _GroupCoordinatorResponseV0Decoder();

  @override
  final RequestEncoder<KRequest> encoder =
      new _GroupCoordinatorRequestV0Encoder();

  final String consumerGroup;
}

class GroupCoordinatorResponseV0 {
  final int errorCode;
  final int coordinatorId;
  final String coordinatorHost;
  final int coordinatorPort;

  GroupCoordinatorResponseV0(this.errorCode, this.coordinatorId,
      this.coordinatorHost, this.coordinatorPort) {
    if (errorCode != KafkaServerError.NoError_) {
      throw new KafkaServerError.fromCode(errorCode, this);
    }
  }
}

class _GroupCoordinatorRequestV0Encoder
    implements RequestEncoder<GroupCoordinatorRequestV0> {
  @override
  List<int> encode(GroupCoordinatorRequestV0 request) {
    var builder = new KafkaBytesBuilder();
    builder.addString(request.consumerGroup);
    return builder.takeBytes();
  }
}

class _GroupCoordinatorResponseV0Decoder
    implements ResponseDecoder<GroupCoordinatorResponseV0> {
  @override
  GroupCoordinatorResponseV0 decode(List<int> data) {
    var reader = new KafkaBytesReader.fromBytes(data);
    var errorCode = reader.readInt16();
    var id = reader.readInt32();
    var host = reader.readString();
    var port = reader.readInt32();

    return new GroupCoordinatorResponseV0(errorCode, id, host, port);
  }
}
