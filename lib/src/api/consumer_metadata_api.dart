part of kafka;

/// ConsumerMetadataRequest as defined in Kafka protocol.
///
/// For convenience implementation of this request handles
/// `ConsumerCoordinatorNotAvailableCode(15)` API error which Kafka returns
/// in case [ConsumerMetadataRequest] is sent for the very first time to this
/// particular broker (when special topic to store consumer offsets does not
/// exist yet).
///
/// It will attempt up to 5 retries with delay in order to fetch metadata.
class ConsumerMetadataRequest extends KafkaRequest {
  final int apiKey = 10;
  final int apiVersion = 0;
  final String consumerGroup;

  /// Creates new instance of ConsumerMetadataRequest.
  ConsumerMetadataRequest(
      KafkaSession session, KafkaHost host, this.consumerGroup)
      : super(session, host);

  /// Sends this request to Kafka server specified in [host].
  Future<ConsumerMetadataResponse> send() async {
    ConsumerMetadataResponse response = await session.send(host, this);

    var retries = 1;
    while (response.errorCode == 15 && retries < 5) {
      var future = new Future.delayed(
          new Duration(seconds: 1 * retries), () => session.send(host, this));

      response = await future;
      retries++;
    }

    if (response.errorCode != 0) {
      throw new KafkaApiError.fromErrorCode(response.errorCode);
    }

    return response;
  }

  /// Converts this request into byte list
  @override
  List<int> toBytes() {
    var builder = new KafkaBytesBuilder.withRequestHeader(
        apiKey, apiVersion, correlationId);

    builder.addString(consumerGroup);

    var body = builder.takeBytes();
    builder.addBytes(body);

    return builder.takeBytes();
  }

  @override
  _createResponse(List<int> data) {
    return ConsumerMetadataResponse.fromData(data, correlationId);
  }
}

/// Result of [ConsumerMetadataRequest] as defined in Kafka protocol.
class ConsumerMetadataResponse {
  final int errorCode;
  final int coordinatorId;
  final String coordinatorHost;
  final int coordinatorPort;

  /// Creates new instance of ConsumerMetadataResponse.
  ConsumerMetadataResponse(this.errorCode, this.coordinatorId,
      this.coordinatorHost, this.coordinatorPort);

  /// Creates response from provided data.
  static ConsumerMetadataResponse fromData(List<int> data, int correlationId) {
    var reader = new KafkaBytesReader.fromBytes(data);
    var size = reader.readInt32();
    assert(size == data.length - 4);

    var receivedCorrelationId = reader.readInt32();
    if (receivedCorrelationId != correlationId) {
      throw new CorrelationIdMismatchError(
          'Original value: $correlationId, received: $receivedCorrelationId');
    }
    var errorCode = reader.readInt16();
    var id = reader.readInt32();
    var host = reader.readString();
    var port = reader.readInt32();

    return new ConsumerMetadataResponse(errorCode, id, host, port);
  }
}
