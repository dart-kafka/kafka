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
    var meta = await session.getMetadata();
    Map<KafkaHost, ProduceRequest> requests = new Map();
    for (var envelope in messages) {
      var topic = meta.getTopicMetadata(envelope.topicName);
      var partition = topic.getPartition(envelope.partitionId);
      var broker = meta.getBroker(partition.leader);
      var host = new KafkaHost(broker.host, broker.port);
      if (!requests.containsKey(host)) {
        requests[host] = new ProduceRequest(requiredAcks, timeout);
      }
      requests[host].addMessages(
          envelope.topicName, envelope.partitionId, envelope.messages);
    }

    var completer = new Completer();
    var futures = new List();
    requests.forEach((h, r) {
      futures.add(session.send(h, r));
    });
    Future.wait(futures).then((List<ProduceResponse> responses) {
      completer.complete(new ProduceResult(responses));
    });

    return completer.future;
  }
}

class ProduceEnvelope {
  final String topicName;
  final int partitionId;
  final List<Message> messages;

  ProduceEnvelope(this.topicName, this.partitionId, this.messages);
}

/// Result of producing messages with [Producer].
class ProduceResult {
  final List<ProduceResponse> responses;
  bool _hasErrors = false;

  bool get hasErrors => _hasErrors;
  Map<String, Map<int, int>> _offsets = new Map();

  Map<String, Map<int, int>> get offsets => _offsets;

  ProduceResult(this.responses) {
    responses.forEach((response) {
      response.topics.forEach((topic) {
        if (_offsets.containsKey(topic.topicName) == false) {
          _offsets[topic.topicName] = new Map();
        }
        topic.partitions.forEach((p) {
          _offsets[topic.topicName][p.partitionId] = p.offset;
          if (p.errorCode > 0) {
            _hasErrors = true;
          }
        });
      });
    });
  }
}
