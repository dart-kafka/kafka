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
  Logger _logger = new Logger('Producer');

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

  Future<ProduceResult> _produce(List<ProduceEnvelope> messages) async {
    var retriesLeft = 3;
    Duration delay;
    ProduceResult result;
    var refreshMetadata = false;

    var topicNames = new Set<String>.from(messages.map((_) => _.topicName));
    ListMultimap<Broker, ProduceEnvelope> messagesByBroker;
    while (retriesLeft > 0) {
      try {
        if (delay is Duration) {
          await new Future.delayed(delay, () => null);
        }
        if (messagesByBroker == null || refreshMetadata) {
          var meta = await session.getMetadata(topicNames,
              invalidateCache: refreshMetadata);

          messagesByBroker =
              new ListMultimap<Broker, ProduceEnvelope>.fromIterable(messages,
                  key: (ProduceEnvelope _) {
            var leaderId = meta
                .getTopicMetadata(_.topicName)
                .getPartition(_.partitionId)
                .leader;
            return meta.getBroker(leaderId);
          });
        }
        _logger
            .fine('Sending ${messagesByBroker.keys.length} ProduceRequest(s).');

        Iterable<Future> responseFutures = new List<Future>.from(
            messagesByBroker.keys.map((broker) => session.send(
                broker,
                new ProduceRequest(
                    requiredAcks, timeout, messagesByBroker[broker]))));

        // responseFutures
        result = await Future.wait(responseFutures).then((responses) =>
            new ProduceResult.fromResponses(
                new List<ProduceResponse>.from(responses)));
        break;
      } catch (error) {
        // Handle "retriable" errors
        if (error is LeaderNotAvailableError ||
            error is NotLeaderForPartitionError ||
            error is RequestTimedOutError ||
            error is NotEnoughReplicasError ||
            error is NotEnoughReplicasAfterAppendError) {
          retriesLeft--;
          refreshMetadata = true;
          delay = (delay == null)
              ? new Duration(seconds: 1)
              : new Duration(seconds: delay.inSeconds * 2);
        } else {
          rethrow;
        }
      }
    }
    return result;
  }
}

/// Result of producing messages with [Producer].
class ProduceResult {
  /// List of actual ProduceResponse objects returned by the server.
  final List<ProduceResponse> responses;

  /// Offsets for latest messages for each topic-partition assigned by the server.
  final Map<String, Map<int, int>> offsets;

  ProduceResult._(this.responses, this.offsets);

  factory ProduceResult.fromResponses(Iterable<ProduceResponse> responses) {
    var offsets = new Map<String, Map<int, int>>();
    for (var r in responses) {
      r.results.forEach((result) {
        offsets.putIfAbsent(result.topicName, () => new Map());
        offsets[result.topicName][result.partitionId] = result.offset;
      });
    }

    return new ProduceResult._(responses, offsets);
  }
}
