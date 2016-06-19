part of kafka;

/// High-level Producer for Kafka.
///
/// Producer encapsulates logic for broker discovery when publishing messages to
/// multiple topic-partitions. It will send as many ProduceRequests as needed
/// based on leader assignment for corresponding topic-partitions.
///
/// Requests will be send in parallel and results will be aggregated in
/// [ProduceResult].
class Producer {
  /// Instance of [KafkaSession] which is used to send requests to Kafka brokers.
  final KafkaSession session;

  /// How many acknowledgements the servers should receive before responding to the request.
  ///
  /// * If it is 0 the server will not send any response.
  /// * If it is 1, the server will wait the data is written to the local log before sending a response.
  /// * If it is -1 the server will block until the message is committed by all in sync replicas before sending a response.
  /// * For any number > 1 the server will block waiting for this number of acknowledgements to occur
  final int requiredAcks;

  /// Maximum time in milliseconds the server can await the receipt of the
  /// number of acknowledgements in [requiredAcks].
  final int timeout;

  /// Creates new instance of [Producer].
  ///
  /// [requiredAcks] specifies how many acknowledgements the servers should
  /// receive before responding to the request.
  ///
  /// [timeout] specifies maximum time in milliseconds the server can await
  /// the receipt of the number of acknowledgements in [requiredAcks].
  Producer(this.session, this.requiredAcks, this.timeout);

  /// Sends messages to Kafka.
  Future<ProduceResult> produce(List<ProduceEnvelope> messages) async {
    var topicNames = messages.map((_) => _.topicName).toSet();
    var meta = await session.getMetadata(topicNames);

    var byBroker =
        new ListMultimap.fromIterable(messages, key: (ProduceEnvelope _) {
      var leaderId =
          meta.getTopicMetadata(_.topicName).getPartition(_.partitionId).leader;
      return meta.getBroker(leaderId);
    });

    var completer = new Completer();
    var futures = new List();
    for (var broker in byBroker.keys) {
      var request = new ProduceRequest(requiredAcks, timeout, byBroker[broker]);
      futures.add(session.send(broker, request));
    }

    Future.wait(futures).then((List<ProduceResponse> responses) {
      completer.complete(new ProduceResult.fromResponses(responses));
    });

    return completer.future;
  }
}

/// Result of producing messages with [Producer].
class ProduceResult {
  /// List of actual ProduceResponse objects returned by the server.
  final List<ProduceResponse> responses;

  /// Indicates whether any of server responses contain errors.
  final bool hasErrors;

  /// Collection of all unique errors returned by the server.
  final Iterable<KafkaServerError> errors;

  /// Offsets for latest messages for each topic-partition assigned by the server.
  final Map<String, Map<int, int>> offsets;

  ProduceResult._(this.responses, Set<KafkaServerError> errors, this.offsets)
      : hasErrors = errors.isNotEmpty,
        errors = new UnmodifiableListView(errors);

  factory ProduceResult.fromResponses(Iterable<ProduceResponse> responses) {
    var errors = new Set<KafkaServerError>();
    var offsets = new Map();
    for (var r in responses) {
      errors.addAll(r.results
          .where((_) => _.errorCode != KafkaServerError.NoError)
          .map((result) => new KafkaServerError(result.errorCode)));
      r.results.forEach((result) {
        offsets.putIfAbsent(result.topicName, () => new Map());
        offsets[result.topicName][result.partitionId] = result.offset;
      });
    }

    return new ProduceResult._(responses, errors, offsets);
  }
}
