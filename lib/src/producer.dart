import 'dart:async';

import 'package:logging/logging.dart';
import 'package:pool/pool.dart';

import 'common.dart';
import 'messages.dart';
import 'produce_api.dart';
import 'serialization.dart';
import 'session.dart';

final Logger _logger = new Logger('Producer');

/// Produces messages to Kafka cluster.
///
/// Automatically discovers leader brokers for each topic-partition to
/// send messages to.
abstract class Producer<K, V> implements StreamSink<ProducerRecord<K, V>> {
  factory Producer(Serializer<K> keySerializer, Serializer<V> valueSerializer,
          ProducerConfig config) =>
      new _Producer(keySerializer, valueSerializer, config);
}

class ProducerRecord<K, V> {
  final String topic;
  final int partition;
  final K key;
  final V value;
  final int timestamp;

  final Completer<ProduceResult> _completer = new Completer();

  ProducerRecord(this.topic, this.partition, this.key, this.value,
      {this.timestamp});

  TopicPartition get topicPartition => new TopicPartition(topic, partition);

  /// The result of publishing this record.
  ///
  /// Returned `Future` is completed with [ProduceResult] on success, otherwise
  /// completed with the produce error.
  Future<ProduceResult> get result => _completer.future;

  void _complete(ProduceResult result) {
    _completer.complete(result);
  }

  void _completeError(error) {
    _completer.completeError(error);
  }
}

class ProduceResult {
  final TopicPartition topicPartition;
  final int offset;
  final int timestamp;

  ProduceResult(this.topicPartition, this.offset, this.timestamp);

  @override
  toString() =>
      'ProduceResult{${topicPartition}, offset: $offset, timestamp: $timestamp}';
}

class _Producer<K, V> implements Producer<K, V> {
  final ProducerConfig config;
  final Serializer<K> keySerializer;
  final Serializer<V> valueSerializer;
  final Session session;

  final StreamController<ProducerRecord<K, V>> _controller =
      new StreamController();

  _Producer(this.keySerializer, this.valueSerializer, this.config)
      : session = new Session(config.bootstrapServers) {
    _logger.info('Producer created with config:');
    _logger.info(config);
  }

  Future _closeFuture;
  @override
  Future close() {
    if (_closeFuture != null) return _closeFuture;

    /// We first close our internal stream controller so that no new records
    /// can be added. Then check if producing is still in progress and wait
    /// for it to complete. And last, after producing is done we close
    /// the session.
    _closeFuture = _controller.close().then((_) {
      return _produceFuture;
    }).then((_) => session.close());
    return _closeFuture;
  }

  @override
  void add(ProducerRecord<K, V> event) {
    _subscribe();
    _controller.add(event);
  }

  @override
  void addError(errorEvent, [StackTrace stackTrace]) {
    /// TODO: Should this throw instead to not allow errors?
    /// Shouldn't really need to implement this method since stream
    /// listener is internal to this class (?)
    _subscribe();
    _controller.addError(errorEvent, stackTrace);
  }

  @override
  Future addStream(Stream<ProducerRecord<K, V>> stream) {
    _subscribe();
    return _controller.addStream(stream);
  }

  @override
  Future get done => close();

  StreamSubscription _subscription;
  void _subscribe() {
    if (_subscription == null) {
      _subscription = _controller.stream.listen(_onData, onDone: _onDone);
    }
  }

  List<ProducerRecord<K, V>> _buffer = new List();
  void _onData(ProducerRecord<K, V> event) {
    _buffer.add(event);
    _resume();
  }

  void _onDone() {
    _logger.fine('Done event received');
  }

  Future _produceFuture;
  void _resume() {
    if (_produceFuture != null) return;
    _logger.fine('New records arrived. Resuming producer.');
    _produceFuture = _produce().whenComplete(() {
      _logger.fine('No more new records. Pausing producer.');
      _produceFuture = null;
    });
  }

  Future _produce() async {
    while (_buffer.isNotEmpty) {
      var records = _buffer;
      _buffer = new List();
      var leaders = await _groupByLeader(records);
      var pools = new Map<Broker, Pool>();
      for (var leader in leaders.keys) {
        pools[leader] = new Pool(config.maxInFlightRequestsPerConnection);
        var requests = _buildRequests(leaders[leader]);
        for (var req in requests) {
          pools[leader].withResource(() => _send(leader, req, leaders[leader]));
        }
      }
      var futures = pools.values.map((_) => _.close());
      await Future.wait(futures);
    }
  }

  Future _send(Broker broker, ProduceRequest request,
      List<ProducerRecord<K, V>> records) {
    return session.send(request, broker.host, broker.port).then((response) {
      Map<TopicPartition, int> offsets = new Map.from(response.results.offsets);
      for (var rec in records) {
        var p = rec.topicPartition;
        rec._complete(
            new ProduceResult(p, offsets[p], response.results[p].timestamp));
        offsets[p]++;
      }
    }).catchError((error) {
      records.forEach((_) {
        _._completeError(error);
      });
    });
  }

  List<ProduceRequest> _buildRequests(List<ProducerRecord<K, V>> records) {
    /// TODO: Split requests by max size.
    var messages = new Map<String, Map<int, List<Message>>>();
    for (var rec in records) {
      var key = keySerializer.serialize(rec.key);
      var value = valueSerializer.serialize(rec.value);
      var timestamp =
          rec.timestamp ?? new DateTime.now().millisecondsSinceEpoch;
      var message = new Message(value, key: key, timestamp: timestamp);
      messages.putIfAbsent(rec.topic, () => new Map());
      messages[rec.topic].putIfAbsent(rec.partition, () => new List());
      messages[rec.topic][rec.partition].add(message);
    }
    var request = new ProduceRequest(config.acks, config.timeoutMs, messages);
    return [request];
  }

  Future<Map<Broker, List<ProducerRecord<K, V>>>> _groupByLeader(
      List<ProducerRecord<K, V>> records) async {
    var topics = records.map((_) => _.topic).toSet().toList(growable: false);
    var metadata = await session.metadata.fetchTopics(topics);
    var result = new Map<Broker, List<ProducerRecord<K, V>>>();
    for (var rec in records) {
      var leader = metadata[rec.topic].partitions[rec.partition].leader;
      var broker = metadata.brokers[leader];
      result.putIfAbsent(broker, () => new List());
      result[broker].add(rec);
    }
    return result;
  }
}

/// Configuration for [Producer].
///
/// The only required setting which must be set is [bootstrapServers],
/// other settings are optional and have default values. Refer
/// to settings documentation for more details.
class ProducerConfig {
  /// A list of host/port pairs to use for establishing the initial
  /// connection to the Kafka cluster. The client will make use of
  /// all servers irrespective of which servers are specified here
  /// for bootstrapping - this list only impacts the initial hosts
  /// used to discover the full set of servers. The values should
  /// be in the form `host:port`.
  /// Since these servers are just used for the initial connection
  /// to discover the full cluster membership (which may change
  /// dynamically), this list need not contain the full set of
  /// servers (you may want more than one, though, in case a
  /// server is down).
  final List<String> bootstrapServers;

  /// The number of acknowledgments the producer requires the leader to have
  /// received before considering a request complete.
  /// This controls the durability of records that are sent.
  final int acks;

  /// Controls the maximum amount of time the server
  /// will wait for acknowledgments from followers to meet the acknowledgment
  /// requirements the producer has specified with the [acks] configuration.
  /// If the requested number of acknowledgments are not met when the timeout
  /// elapses an error is returned by the server. This timeout is measured on the
  /// server side and does not include the network latency of the request.
  final int timeoutMs;

  /// Setting a value greater than zero will cause the client to resend any
  /// record whose send fails with a potentially transient error.
  final int retries;

  /// An id string to pass to the server when making requests.
  /// The purpose of this is to be able to track the source of requests
  /// beyond just ip/port by allowing a logical application name to be
  /// included in server-side request logging.
  final String clientId;

  /// The maximum size of a request in bytes. This is also effectively a
  /// cap on the maximum record size. Note that the server has its own
  /// cap on record size which may be different from this.
  final int maxRequestSize;

  /// The maximum number of unacknowledged requests the client will
  /// send on a single connection before blocking. Note that if this
  /// setting is set to be greater than 1 and there are failed sends,
  /// there is a risk of message re-ordering due to retries (i.e.,
  /// if retries are enabled).
  final int maxInFlightRequestsPerConnection;

  ProducerConfig({
    this.bootstrapServers,
    this.acks = 1,
    this.timeoutMs = 30000,
    this.retries = 0,
    this.clientId = '',
    this.maxRequestSize = 1048576,
    this.maxInFlightRequestsPerConnection = 5,
  }) {
    assert(bootstrapServers != null);
  }

  @override
  String toString() => '''
ProducerConfig(
  bootstrapServers: $bootstrapServers, 
  acks: $acks, 
  timeoutMs: $timeoutMs,
  retries: $retries,
  clientId: $clientId,
  maxRequestSize: $maxRequestSize,
  maxInFlightRequestsPerConnection: $maxInFlightRequestsPerConnection
)
''';
}
