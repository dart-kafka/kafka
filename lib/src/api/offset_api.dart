part of kafka;

/// ## OffsetRequest as defined in Kafka protocol spec.
///
/// This is a low-level API object and requires good knowledge of Kafka protocol.
class OffsetRequest extends KafkaRequest {
  /// Instance of [KafkaClient].
  final KafkaClient client;

  /// Kafka server to which this request will be directed.
  final KafkaHost host;

  /// Unique ID assigned to the [host] within Kafka cluster.
  final int replicaId;

  /// Creates new instance of OffsetRequest.
  OffsetRequest(this.client, this.host, this.replicaId);

  /// Sends this request to the server.
  Future<OffsetResponse> send() {
    //
  }

  /// Converts this request to byte array.
  @override
  List<int> toBytes() {
    // TODO: implement toBytes
  }
}

class OffsetResponse {
  ///
}
