import 'common.dart';
import 'io.dart';
import 'errors.dart';

/// Kafka GroupCoordinator request.
class GroupCoordinatorRequest extends KRequest<GroupCoordinatorResponse> {
  @override
  final int apiKey = ApiKey.groupCoordinator;

  /// The name of consumer group to fetch coordinator details for.
  final String group;

  GroupCoordinatorRequest(this.group);

  @override
  final ResponseDecoder<GroupCoordinatorResponse> decoder =
      const _GroupCoordinatorResponseDecoder();

  @override
  final RequestEncoder<KRequest> encoder =
      const _GroupCoordinatorRequestEncoder();
}

class GroupCoordinatorResponse {
  final int error;
  final int coordinatorId;
  final String coordinatorHost;
  final int coordinatorPort;

  GroupCoordinatorResponse(this.error, this.coordinatorId, this.coordinatorHost,
      this.coordinatorPort) {
    if (error != Errors.NoError) {
      throw new KafkaError.fromCode(error, this);
    }
  }

  Broker get coordinator =>
      new Broker(coordinatorId, coordinatorHost, coordinatorPort);
}

class _GroupCoordinatorRequestEncoder
    implements RequestEncoder<GroupCoordinatorRequest> {
  const _GroupCoordinatorRequestEncoder();

  @override
  List<int> encode(GroupCoordinatorRequest request, int version) {
    assert(version == 0,
        'Only v0 of GroupCoordinator request is supported by the client.');
    var builder = new KafkaBytesBuilder();
    builder.addString(request.group);
    return builder.takeBytes();
  }
}

class _GroupCoordinatorResponseDecoder
    implements ResponseDecoder<GroupCoordinatorResponse> {
  const _GroupCoordinatorResponseDecoder();

  @override
  GroupCoordinatorResponse decode(List<int> data) {
    var reader = new KafkaBytesReader.fromBytes(data);
    var error = reader.readInt16();
    var id = reader.readInt32();
    var host = reader.readString();
    var port = reader.readInt32();
    return new GroupCoordinatorResponse(error, id, host, port);
  }
}
