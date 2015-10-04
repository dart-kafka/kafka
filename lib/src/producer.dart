part of kafka;

/// High-level Producer for Kafka.
///
/// This class encapsulates some details of how [ProduceRequest]s work in Kafka
/// and provides simple API for implementing producers.
///
/// _It is recommended to use this class instead of [ProduceRequest] directly._
class KafkaProducer {
  /// Instance of [KafkaClient] which is used to send requests to Kafka brokers.
  final KafkaClient client;

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

  /// Map to keep track of which request should go to which broker.
  Map<KafkaHost, ProduceRequest> _requests = new Map();

  /// Creates new instance of [KafkaProducer].
  ///
  /// [requiredAcks] specifies how many acknowledgements the servers should
  /// receive before responding to the request.
  ///
  /// [timeout] specifies maximum time in milliseconds the server can await
  /// the receipt of the number of acknowledgements in [requiredAcks].
  KafkaProducer(this.client, this.requiredAcks, this.timeout);

  /// Adds messages to be sent in a [ProduceRequest] to Kafka.
  ///
  /// This method will autodetect which broker is currently a leader for
  /// [topicName] and [partitionId] so that actual request will be sent to
  /// that particular node.
  Future addMessages(
      String topicName, partitionId, List<Message> messages) async {
    var metadata = await client.getMetadata();
    var topic = metadata.getTopicMetadata(topicName);
    var partition = topic.getPartition(partitionId);

    var broker = metadata.getBroker(partition.leader);
    var host = new KafkaHost(broker.host, broker.port);
    _getRequestForHost(host).addMessages(topicName, partitionId, messages);
  }

  /// Sends produce data to Kafka.
  ///
  /// Depending on number of topics and partitions in the produce data this may
  /// send multiple [ProduceRequest]s to Kafka.
  ///
  /// This method will wait for all [ProduceRequest]s to finish, aggregate the
  /// results and return instance of [ProduceResult].
  Future<ProduceResult> send() async {
    var futures = _requests.values.map((r) => r.send());
    var completer = new Completer();
    Future.wait(futures).then((List<ProduceResponse> responses) {
      completer.complete(new ProduceResult(responses));
    });

    return completer.future;
  }

  ProduceRequest _getRequestForHost(KafkaHost host) {
    if (_requests.containsKey(host) == false) {
      _requests[host] = new ProduceRequest(client, host, requiredAcks, timeout);
    }

    return _requests[host];
  }
}

/// Result of [KafkaProducer.send()] call.
///
/// Provides convenience layer on top of Kafka API [ProduceResponse]:
/// * Aggregates information about Kafka API errors.
/// * Provides easy access to failed requests data (if any)
class ProduceResult {
  bool _hasErrors = false;

  bool get hasErrors => _hasErrors;

  ProduceResult(List<ProduceResponse> responses) {
    responses.forEach((response) {
      response.topics.forEach((topic) {
        topic.partitions.forEach((p) {
          if (p.errorCode > 0) {
            _hasErrors = true;
          }
        });
      });
    });
  }
}
