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

  /// Sends messages to Kafka with "at least once" guarantee.
  ///
  /// Producer will attempt to retry requests when Kafka server returns any of
  /// the retriable errors. See [ProduceResult.hasRetriableErrors] for details.
  ///
  /// In case of such errors producer will attempt to re-send **all the messages**
  /// and this may lead to duplicate records in the stream (therefore
  /// "at least once" guarantee).
  ///
  /// If server returns errors which can not be retried then returned future will
  /// be completed with [ProduceError]. One can still access `ProduceResult` from
  /// it.
  ///
  /// In case of any non-protocol errors returned future will complete with actual
  /// error that was thrown.
  Future<ProduceResult> produce(List<ProduceEnvelope> messages) {
    return _produce(messages);
  }

  Future<ProduceResult> _produce(List<ProduceEnvelope> messages,
      {bool refreshMetadata: false,
      int retryTimes: 3,
      Duration retryInterval: const Duration(seconds: 1)}) async {
    var topicNames = new Set<String>.from(messages.map((_) => _.topicName));
    var meta =
        await session.getMetadata(topicNames, invalidateCache: refreshMetadata);

    var byBroker = new ListMultimap<Broker, ProduceEnvelope>.fromIterable(
        messages, key: (ProduceEnvelope _) {
      var leaderId =
          meta.getTopicMetadata(_.topicName).getPartition(_.partitionId).leader;
      return meta.getBroker(leaderId);
    });
    kafkaLogger.fine('Producer: sending ProduceRequests');

    Iterable<Future> futures = new List<Future>.from(byBroker.keys.map(
        (broker) => session.send(broker,
            new ProduceRequest(requiredAcks, timeout, byBroker[broker]))));

    var result = await Future.wait(futures).then((responses) =>
        new ProduceResult.fromResponses(
            new List<ProduceResponse>.from(responses)));

    if (!result.hasErrors) return result;
    if (retryTimes <= 0) return result;

    if (result.hasRetriableErrors) {
      kafkaLogger.warning(
          'Producer: server returned errors which can be retried. All returned errors are: ${result.errors}');
      kafkaLogger.info(
          'Producer: will retry after ${retryInterval.inSeconds} seconds.');
      var retriesLeft = retryTimes - 1;
      var newInterval = new Duration(seconds: retryInterval.inSeconds * 2);
      return new Future<ProduceResult>.delayed(
          retryInterval,
          () => _produce(messages,
              refreshMetadata: true,
              retryTimes: retriesLeft,
              retryInterval: newInterval));
    } else if (result.hasErrors) {
      throw new ProduceError(result);
    } else {
      return result;
    }
  }
}

/// Exception thrown in case when server returned errors in response to
/// `Producer.produce()`.
class ProduceError implements Exception {
  final ProduceResult result;

  ProduceError(this.result);

  @override
  toString() => 'ProduceError: ${result.errors}';
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
    var offsets = new Map<String, Map<int, int>>();
    for (var r in responses) {
      var er = r.results
          .where((_) => _.errorCode != KafkaServerError.NoError)
          .map((result) => new KafkaServerError(result.errorCode));
      errors.addAll(new Set<KafkaServerError>.from(er));
      r.results.forEach((result) {
        offsets.putIfAbsent(result.topicName, () => new Map());
        offsets[result.topicName][result.partitionId] = result.offset;
      });
    }

    return new ProduceResult._(responses, errors, offsets);
  }

  /// Returns `true` if this result contains server error with specified [code].
  bool hasError(int code) => errors.contains(new KafkaServerError(code));

  /// Returns `true` if at least one server error in this result can be retried.
  bool get hasRetriableErrors {
    return hasError(KafkaServerError.LeaderNotAvailable) ||
        hasError(KafkaServerError.NotLeaderForPartition) ||
        hasError(KafkaServerError.RequestTimedOut) ||
        hasError(KafkaServerError.NotEnoughReplicasCode) ||
        hasError(KafkaServerError.NotEnoughReplicasAfterAppendCode);
  }
}
