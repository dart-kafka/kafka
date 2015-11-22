part of kafka;

/// Session responsible for communication with Kafka server(s).
///
/// This is a central hub of this library as it handles socket connections
/// to Kafka brokers and orchestrates all API communications.
class KafkaSession {
  /// List of Kafka brokers which are used as initial contact points.
  final Queue<KafkaHost> contactHosts;
  final Map<KafkaHost, Socket> _sockets = new Map();
  final Map<KafkaHost, StreamSubscription> _subscriptions = new Map();

  Map<KafkaRequest, Completer> _inflightRequests = new Map();
  Map<KafkaHost, List<int>> _buffers = new Map();
  Map<KafkaHost, int> _sizes = new Map();
  MetadataResponse _metadata;

  /// Creates new session.
  ///
  /// [contactHosts] will be used to fetch Kafka metadata information. At least
  /// one is required. However for production consider having more than 1.
  /// In case of one of the hosts is temporarily unavailable the session will
  /// rotate them until sucessful response is returned. Error will be thrown
  /// when all of the default hosts are unavailable.
  KafkaSession(List<KafkaHost> contactHosts)
      : contactHosts = new Queue.from(contactHosts);

  /// Fetches Kafka server metadata. If [topicNames] is null then metadata for
  /// all topics will be returned.
  ///
  /// This is a wrapper around Kafka API [MetadataRequest].
  /// Result will also contain information about all brokers in the Kafka cluster.
  /// See [MetadataResponse] for details.
  Future<MetadataResponse> getMetadata(
      {List<String> topicNames, bool invalidateCache: false}) async {
    // TODO: actually rotate default hosts on failure.
    if (invalidateCache) _metadata = null;

    if (_metadata == null) {
      var currentHost = _getCurrentDefaultHost();
      var request = new MetadataRequest(topicNames);
      _metadata = await this.send(currentHost, request);
    }

    return _metadata;
  }

  /// Fetches metadata for specified [consumerGroup].
  ///
  /// It handles `ConsumerCoordinatorNotAvailableCode(15)` API error which Kafka
  /// returns in case [ConsumerMetadataRequest] is sent for the very first time
  /// to this particular broker (when special topic to store consumer offsets
  /// does not exist yet).
  ///
  /// It will attempt up to 5 retries (with delay) in order to fetch metadata.
  Future<ConsumerMetadataResponse> getConsumerMetadata(
      String consumerGroup) async {
    // TODO: rotate default hosts.
    var currentHost = _getCurrentDefaultHost();
    var request = new ConsumerMetadataRequest(consumerGroup);

    var response = await this.send(currentHost, request);
    var retries = 1;
    while (response.errorCode == 15 && retries < 5) {
      var future = new Future.delayed(new Duration(seconds: 1 * retries),
          () => this.send(currentHost, request));

      response = await future;
      retries++;
    }

    if (response.errorCode != 0) {
      throw new KafkaApiError.fromErrorCode(response.errorCode);
    }

    return response;
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

  /// Closes this session and terminates all open socket connections.
  ///
  /// After session has been closed it can't be used or re-opened.
  Future close() async {
    for (var h in _sockets.keys) {
      await _subscriptions[h].cancel();
      _sockets[h].destroy();
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
    return contactHosts.first;
  }

  // void _rotateDefaultHosts() {
  //   var current = defaultHosts.removeFirst();
  //   defaultHosts.addLast(current);
  // }

  Future<Socket> _getSocketForHost(KafkaHost host) async {
    if (!_sockets.containsKey(host)) {
      var s = await Socket.connect(host.host, host.port);
      _buffers[host] = new List();
      _sizes[host] = -1;
      _subscriptions[host] = s.listen((d) => _handleData(host, d));
      _sockets[host] = s;
      _sockets[host].setOption(SocketOption.TCP_NODELAY, true);
    }

    return _sockets[host];
  }
}
