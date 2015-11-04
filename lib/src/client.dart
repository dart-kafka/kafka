part of kafka;

/// Client responsible for communication with Kafka server(s).
///
/// This is a central hub of this library. All low-level requests as well as
/// high-level abstractions for producers and consumers depend on this class.
///
class KafkaClient {
  final Queue<KafkaHost> defaultHosts;
  final Map<KafkaHost, Socket> sockets = new Map();
  final Map<KafkaHost, StreamSubscription> subscriptions = new Map();

  Map<KafkaRequest, Completer> _inflightRequests = new Map();

  Map<KafkaHost, List<int>> _buffers = new Map();
  Map<KafkaHost, int> _sizes = new Map();

  /// Creates new instance of KafkaClient.
  ///
  /// [defaultHosts] will be used to fetch Kafka metadata information. At least
  /// one is required. However for production consider having more than 1.
  /// In case of one of the hosts is temporarily unavailable the client will
  /// rotate them until sucessful response is returned. Error will be thrown
  /// when all of the default hosts are unavailable.
  KafkaClient(List<KafkaHost> defaultHosts)
      : defaultHosts = new Queue.from(defaultHosts);

  /// Fetches Kafka server metadata. If [topicNames] is null then metadata for
  /// all topics will be returned.
  ///
  /// This is a wrapper around Kafka API [MetadataRequest].
  /// Result will also contain information about all brokers in the Kafka cluster.
  /// See [MetadataResponse] for details.
  ///
  /// TODO: actually rotate default hosts on failure.
  /// TODO: cache metadata results.
  Future<MetadataResponse> getMetadata(
      [List<String> topicNames, bool invalidateCache = false]) async {
    var currentHost = _getCurrentDefaultHost();
    var request = new MetadataRequest(this, currentHost, topicNames);

    return request.send();
  }

  /// Fetches metadata for specified [consumerGroup].
  ///
  /// TODO: rotate default hosts.
  Future<ConsumerMetadataResponse> getConsumerMetadata(
      String consumerGroup) async {
    var currentHost = _getCurrentDefaultHost();
    var request = new ConsumerMetadataRequest(this, currentHost, consumerGroup);
    return request.send();
  }

  /// Sends request to specified [KafkaHost].
  Future<dynamic> send(KafkaHost host, KafkaRequest request) async {
    var socket = await _getSocketForHost(host);
    Completer completer = new Completer();
    _inflightRequests[request] = completer;
    // TODO: add timeout for how long to wait for response.
    // TODO: add error handling.
    socket.add(request.toBytes());

    return completer.future;
  }

  Future close() async {
    for (var h in sockets.keys) {
      await subscriptions[h].cancel();
      sockets[h].destroy();
    }
  }

  void _handleData(KafkaHost host, List<int> d) {
    var buffer = _buffers[host];

    buffer.addAll(d);
    if (buffer.length >= 4 && _sizes[host] == -1) {
      var sizeBytes = buffer.sublist(0, 4);
      var reader = new KafkaBytesReader.fromBytes(sizeBytes);
      _sizes[host] = reader.readInt32();
    }

    var extra;
    if (buffer.length > _sizes[host] + 4) {
      _logger?.finest('Extra data: ${buffer.length}, size: ${_sizes[host]}');
      extra = buffer.sublist(_sizes[host] + 4);
      buffer.removeRange(_sizes[host] + 4, buffer.length);
    }

    if (buffer.length == _sizes[host] + 4) {
      var header = buffer.sublist(4, 8);
      var reader = new KafkaBytesReader.fromBytes(header);
      var correlationId = reader.readInt32();
      var request = _inflightRequests.keys
          .firstWhere((r) => r.correlationId == correlationId);
      var completer = _inflightRequests[request];
      completer.complete(request._createResponse(buffer));
      _inflightRequests.remove(request);
      buffer.clear();
      _sizes[host] = -1;
      if (extra is List && extra.isNotEmpty) {
        _handleData(host, extra);
      }
    }
  }

  KafkaHost _getCurrentDefaultHost() {
    return defaultHosts.first;
  }

  // void _rotateDefaultHosts() {
  //   var current = defaultHosts.removeFirst();
  //   defaultHosts.addLast(current);
  // }

  Future<Socket> _getSocketForHost(KafkaHost host) async {
    if (!sockets.containsKey(host)) {
      var s = await Socket.connect(host.host, host.port);
      _buffers[host] = new List();
      _sizes[host] = -1;
      subscriptions[host] = s.listen((d) => _handleData(host, d));
      sockets[host] = s;
      sockets[host].setOption(SocketOption.TCP_NODELAY, true);
    }

    return sockets[host];
  }
}
