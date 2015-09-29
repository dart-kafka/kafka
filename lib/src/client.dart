part of kafka;

class KafkaClient {
  String protocolVersion;
  Queue<KafkaHost> defaultHosts;
  Map<KafkaHost, Socket> sockets = new Map();
  Map<KafkaHost, StreamSubscription> subscriptions = new Map();

  /// Creates new instance of KafkaClient.
  ///
  /// The [defaultHosts] will be used to fetch Kafka metadata information. At least
  /// one is required. However for production consider having more than 1.
  /// In case of one of the hosts is temporarily unavailable the client will
  /// rotate them until sucessful response is returned. Error will be thrown
  /// when all of the default hosts are unavailable.
  KafkaClient(this.protocolVersion, List<KafkaHost> defaultHosts) {
    this.defaultHosts = new Queue.from(defaultHosts);
  }

  /// Fetches Kafka server metadata. If [topicNames] is null then metadata for
  /// all topics will be returned.
  ///
  /// This is a wrapper around Kafka API [MetadataRequest].
  /// Result will also contain information about all brokers in the Kafka cluster.
  /// See [MetadataResponse] for details.
  Future<MetadataResponse> getMetadata(
      [List<String> topicNames, bool invalidateCache = false]) async {
    var currentHost = _getCurrentDefaultHost();
    var request = new MetadataRequest(this, currentHost, topicNames);

    return request.send();
  }

  /// Sends request to specified [KafkaHost].
  Future<List<int>> send(KafkaHost host, KafkaRequest request) async {
    var socket = await _getSocketForHost(host);
    Completer completer = new Completer();
    List<int> _data = new List();
    int size = -1;
    subscriptions[host].onData((d) {
      _data.addAll(d);
      if (_data.length >= 4) {
        var sizeBytes = _data.sublist(0, 4);
        var reader = new KafkaBytesReader.fromBytes(sizeBytes);
        size = reader.readInt32();
      }

      if (size == _data.length - 4) {
        completer.complete(_data);
      }
    });
    socket.add(request.toBytes());

    return completer.future;
  }

  KafkaHost _getCurrentDefaultHost() {
    return defaultHosts.first;
  }

  void _rotateDefaultHosts() {
    var current = defaultHosts.removeFirst();
    defaultHosts.addLast(current);
  }

  Future<Socket> _getSocketForHost(KafkaHost host) async {
    if (!sockets.containsKey(host)) {
      var s = await Socket.connect(host.host, host.port);
      subscriptions[host] = s.listen(null);
      sockets[host] = s;
      sockets[host].setOption(SocketOption.TCP_NODELAY, true);
    }

    return sockets[host];
  }
}
