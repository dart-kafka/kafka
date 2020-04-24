import 'dart:async';
import 'package:logging/logging.dart';
import 'consumer_streamiterator.dart';
import 'common.dart';
import 'consumer_group.dart';
import 'consumer_offset_api.dart';
import 'errors.dart';
import 'fetch_api.dart';
import 'group_membership_api.dart';
import 'offset_master.dart';
import 'serialization.dart';
import 'session.dart';
import 'util/group_by.dart';

final Logger _logger = new Logger('Consumer');

/// Consumes messages from Kafka cluster.
///
/// Consumer interacts with the server to allow multiple members of the same group
/// load balance consumption by distributing topics and partitions evenly across
/// all members.
///
/// ## Subscription and rebalance
///
/// Before it can start consuming messages, Consumer must [subscribe] to a set
/// of topics. Every instance subscribes as a distinct member of it's [group].
/// Each member receives assignment which contains a unique subset of topics and
/// partitions it must consume. So if there is only one member then it gets
/// assigned all topics and partitions when subscribed.
///
/// When another member wants to join the same group a rebalance is triggered by
/// the Kafka server. During rebalance all members must rejoin the group and
/// receive their updated assignments. Rebalance is handled transparently by
/// this client and does not require any additional action.
///
/// ## Usage example
///
///     TODO: write an example
abstract class Consumer<K, V> {
  /// The consumer group name.
  String get group;

  /// Starts polling Kafka servers for new messages.
  ///
  /// Must first call [subscribe] to indicate which topics must be consumed.
  StreamIterator<ConsumerRecords<K, V>> poll();

  /// Subscribe this consumer to a set of [topics].
  ///
  /// This function evaluates lazily, subscribing to the specified topics only
  /// when [poll] is called.
  void subscribe(List<String> topics);

  /// Unsubscribes from all currently assigned partitions and leaves
  /// consumer group.
  ///
  /// Unsubscribe triggers rebalance of all existing members of this consumer
  /// group.
  Future unsubscribe();

  /// Commits current offsets to the server.
  Future commit();

  /// Seek to the first offset for all of the currently assigned partitions.
  ///
  /// This function evaluates lazily, seeking to the first offset in all
  /// partitions only when [poll] is called.
  ///
  /// Requires active subscription, see [subscribe] for more details.
  void seekToBeginning();

  /// Seek to the last offset for all of the currently assigned partitions.
  ///
  /// This function evaluates lazily, seeking to the last offset in all
  /// partitions only when [poll] is called.
  ///
  /// Requires active subscription, see [subscribe] for more details.
  void seekToEnd();

  factory Consumer(String group, Deserializer<K> keyDeserializer,
      Deserializer<V> valueDeserializer, Session session) {
    return new _ConsumerImpl(
        group, keyDeserializer, valueDeserializer, session);
  }
}

/// Defines type of consumer state function.
typedef Future _ConsumerState();

/// Default implementation of Kafka consumer.
///
/// Implements a finite state machine which is started by a call to [poll].
class _ConsumerImpl<K, V> implements Consumer<K, V> {
  static const int DEFAULT_MAX_BYTES = 36864;
  static const int DEFAULT_MAX_WAIT_TIME = 10000;
  static const int DEFAULT_MIN_BYTES = 1;

  final Session session;
  final Deserializer<K> keyDeserializer;
  final Deserializer<V> valueDeserializer;
  final int requestMaxBytes;

  final ConsumerGroup _group;

  _ConsumerState _activeState;

  _ConsumerImpl(
      String group, this.keyDeserializer, this.valueDeserializer, this.session,
      {int requestMaxBytes})
      : _group = new ConsumerGroup(session, group),
        requestMaxBytes = requestMaxBytes ?? DEFAULT_MAX_BYTES;

  /// The consumer group name.
  String get group => _group.name;

  GroupSubscription _subscription;

  /// Current consumer subscription.
  GroupSubscription get subscription => _subscription;

  /// List of topics to subscribe to when joining the group.
  ///
  /// Set by initial call to [subscribe] and used during initial
  /// subscribe and possible resubscriptions.
  List<String> _topics;

  StreamController<ConsumerRecords<K, V>> _streamController;
  ConsumerStreamIterator<K, V> _streamIterator;

  /// Whether user canceled stream subscription.
  ///
  /// This triggers shutdown of polling phase.
  bool _isCanceled = false;

  /// Whether resubscription is required due to rebalance event received from
  /// the server.
  bool _resubscriptionNeeded = false;

  @override
  void subscribe(List<String> topics) {
    assert(_subscription == null, 'Already subscribed.');
    _topics = new List.from(topics, growable: false);
  }

  /// State of this consumer during (re)subscription.
  ///
  /// Consumer enters this state initially when listener is added to the stream
  /// and may re-enter this state in case of a rebalance event triggerred by
  /// the server.
  Future _resubscribeState() {
    _logger
        .info('Subscribing to topics ${_topics} as a member of group $group');
    var protocols = [new GroupProtocol.roundrobin(0, _topics.toSet())];
    return _group.join(30000, 3000, '', 'consumer', protocols).then((result) {
      // TODO: resume heartbeat timer.
      _subscription = result;
      _resubscriptionNeeded = false;
      _logger.info('Subscription result: ${subscription}.');
      // Switch to polling state
      _activeState = _pollState;
    });
  }

  @override
  StreamIterator<ConsumerRecords<K, V>> poll() {
    assert(_topics != null,
        'No topics set for subscription. Must first call subscribe().');
    assert(_streamController == null, 'Already polling.');

    _streamController = new StreamController<ConsumerRecords<K, V>>(
        onListen: onListen, onCancel: onCancel);
    _streamIterator =
        new ConsumerStreamIterator<K, V>(_streamController.stream);

    return _streamIterator;
  }

  /// Starts execution of state machine.
  ///
  /// Returned future completes whenever there is no active state
  /// (execution completed) or unhandled error occured.
  Future _run() async {
    while (_activeState != null) {
      await _activeState();
    }
  }

  /// Only set in initial stream controler. Rebalance events create new streams
  /// but we don't set onListen callback on those since our state machine
  /// is already running.
  void onListen() {
    // Start polling only after there is active listener.
    _activeState = _resubscribeState;
    _run().catchError((error, stackTrace) {
      _streamController.addError(error, stackTrace);
    }).whenComplete(() {
      // TODO: ensure cleanup here, e.g. shutdown heartbeats
      var closeFuture = _streamController.close();
      _streamController = null;
      _streamIterator = null;
      return closeFuture;
    });
  }

  void onCancel() {
    _isCanceled = true;
    // Listener canceled subscription so we need to drop any records waiting
    // to be processed.
    _waitingRecords.values.forEach((_) {
      _.ack();
    });
  }

  /// Poll state of this consumer's state machine.
  ///
  /// Consumer enters this state when [poll] is executed and stays in this state
  /// until:
  ///
  /// - user cancels the poll, results in "clean exit".
  /// - a rebalance error received from the server, which transitions state machine to
  ///   the [_resubscribeState].
  /// - an unhandled error occurs, adds error to the stream.
  Future _pollState() async {
    return _poll().catchError(
      (error) {
        // Switch to resubscribe state because server is performing rebalance.
        _resubscriptionNeeded = true;
      },
      test: isRebalanceError,
    ).whenComplete(() {
      // Check if resubscription is needed in case there were rebalance
      // errors from either offset commit or heartbeat requests.
      if (_resubscriptionNeeded) {
        _logger.fine("switch to resubscribe state");
        // Switch to resubscribe state.
        _activeState = _resubscribeState;
        // Create new stream controller and attach to the stream iterator.
        // This cancels subscription on existing stream and prevents delivery
        // of any in-flight events to the listener. It also clears uncommitted
        // offsets to prevent offset commits during rebalance.

        // Remove onCancel callback on existing controller.
        _streamController.onCancel = null;
        _streamController =
            StreamController<ConsumerRecords<K, V>>(onCancel: onCancel);
        _streamIterator.attachStream(_streamController.stream);
      }
    });
  }

  /// Returns `true` if [error] requires resubscription.
  bool isRebalanceError(error) =>
      error is RebalanceInProgressError || error is UnknownMemberIdError;

  /// Internal polling method.
  Future _poll() async {
    var offsets = await _fetchOffsets(subscription);
    _logger.fine('Polling started from following offsets: ${offsets}');
    Map<Broker, List<ConsumerOffset>> leaders =
        await _fetchPartitionLeaders(subscription, offsets);

    List<Future> brokerPolls = new List();
    for (var broker in leaders.keys) {
      brokerPolls.add(_pollBroker(broker, leaders[broker]));
    }
    await Future.wait(brokerPolls);
  }

  /// Stores references to consumer records in each polling broker that this
  /// consumer is currently waiting to be processed.
  /// The `onCancel` callback acknowledges all of these so that polling can
  /// shutdown gracefully.
  final Map<Broker, ConsumerRecords<K, V>> _waitingRecords = Map();

  Future _pollBroker(Broker broker, List<ConsumerOffset> initialOffsets) async {
    Map<TopicPartition, ConsumerOffset> currentOffsets = Map.fromIterable(
        initialOffsets,
        key: (offset) => offset.topicPartition);

    while (true) {
      if (_isCanceled || _resubscriptionNeeded) {
        _logger.fine('Stoping poll on $broker.');
        break;
      }

      _logger.fine('Sending poll request on $broker');
      var request =
          _buildRequest(currentOffsets.values.toList(growable: false));
      var response = await session.send(request, broker.host, broker.port);

      var records = recordsFromResponse(response.results);

      _logger
          .fine('response from $broker has ${records.records.length} records');

      if (records.records.isEmpty) continue; // empty response, continue polling

      for (var rec in records.records) {
        currentOffsets[rec.topicPartition] =
            ConsumerOffset(rec.topic, rec.partition, rec.offset, '');
      }

      _waitingRecords[broker] = records;
      _streamController.add(records);
      await records.future;
    }
  }

  ConsumerRecords<K, V> recordsFromResponse(List<FetchResult> results) {
    var records = results.expand((result) {
      return result.messages.keys.map((offset) {
        var message = result.messages[offset];
        var key = keyDeserializer.deserialize(message.key);
        var value = valueDeserializer.deserialize(message.value);
        return ConsumerRecord<K, V>(result.topic, result.partition, offset, key,
            value, message.timestamp);
      });
    }).toList(growable: false);
    return ConsumerRecords<K, V>(records);
  }

  /// Fetches current consumer offsets from the server.
  ///
  /// Checks whether current offsets are valid by comparing to earliest
  /// available offsets in the topics. Resets current offset if it's value is
  /// lower than earliest available in the partition.
  Future<List<ConsumerOffset>> _fetchOffsets(
      GroupSubscription subscription) async {
    _logger.finer('Fetching offsets for ${group}');
    var currentOffsets =
        await _group.fetchOffsets(subscription.assignment.partitionsAsList);
    var offsetMaster = new OffsetMaster(session);
    var earliestOffsets = await offsetMaster
        .fetchEarliest(subscription.assignment.partitionsAsList);

    List<ConsumerOffset> resetNeeded = new List();
    for (var earliest in earliestOffsets) {
      // Current consumer offset can be either -1 or a value >= 0, where
      // `-1` means that no committed offset exists for this partition.
      //
      var current = currentOffsets.firstWhere((_) =>
          _.topic == earliest.topic && _.partition == earliest.partition);
      if (current.offset + 1 < earliest.offset) {
        // reset to earliest
        _logger.warning('Current consumer offset (${current.offset}) is less '
            'than earliest available for partition (${earliest.offset}). '
            'This may indicate that consumer missed some records in ${current.topicPartition}. '
            'Resetting this offset to earliest.');
        resetNeeded.add(current.copy(
            offset: earliest.offset - 1, metadata: 'resetToEarliest'));
      }
    }

    if (resetNeeded.isNotEmpty) {
      await _group.commitOffsets(resetNeeded, subscription: subscription);
      return _group.fetchOffsets(subscription.assignment.partitionsAsList);
    } else {
      return currentOffsets;
    }
  }

  FetchRequest _buildRequest(List<ConsumerOffset> offsets) {
    var request = new FetchRequest(DEFAULT_MAX_WAIT_TIME, DEFAULT_MIN_BYTES);
    for (var offset in offsets) {
      request.add(offset.topicPartition,
          new FetchData(offset.offset + 1, requestMaxBytes));
    }
    return request;
  }

  Future<Map<Broker, List<ConsumerOffset>>> _fetchPartitionLeaders(
      GroupSubscription subscription, List<ConsumerOffset> offsets) async {
    var topics = subscription.assignment.topics;
    var topicsMeta = await session.metadata.fetchTopics(topics);
    var brokerOffsets = offsets
        .where((_) =>
            subscription.assignment.partitionsAsList.contains(_.topicPartition))
        .toList(growable: false);
    return groupBy<Broker, ConsumerOffset>(brokerOffsets, (_) {
      var leaderId = topicsMeta[_.topic].partitions[_.partition].leader;
      return topicsMeta.brokers[leaderId];
    });
  }

  @override
  Future unsubscribe() {
    // TODO: implement unsubscribe
    return null;
  }

  @override
  Future commit() async {
    // TODO: What should happen in case of an unexpected error in here?
    // This should probably cancel polling and complete returned future
    // with this unexpected error.
    assert(_streamIterator != null);
    assert(_streamIterator.current != null);
    _logger.fine('Committing offsets.');
    var offsets = _streamIterator.offsets;
    if (offsets.isNotEmpty) {
      return _group
          .commitOffsets(_streamIterator.offsets, subscription: _subscription)
          .catchError((error) {
        /// It is possible to receive a rebalance error in response to OffsetCommit
        /// request. We set `_resubscriptionNeeded` to `true` so that next cycle
        /// of polling can exit and switch to [_resubscribeState].
        _logger.warning(
            'Received $error on offset commit. Requiring resubscription.');
        _resubscriptionNeeded = true;
      }, test: isRebalanceError).whenComplete(() {
        _logger.fine('Done committing offsets.');

        /// Clear accumulated offsets regardless of the result of OffsetCommit.
        /// If commit successeded clearing current offsets is safe.
        /// If commit failed we either go to resubscribe state which requires re-fetch
        /// of offsets, or we have unexpected error so we need to shutdown polling and
        /// cleanup internal state.
        _streamIterator.clearOffsets();
      });
    }
  }

  @override
  void seekToBeginning() {
    // TODO: implement seekToBeginning
  }

  @override
  void seekToEnd() {
    // TODO: implement seekToEnd
  }
}

class ConsumerRecord<K, V> {
  final String topic;
  final int partition;
  final int offset;
  final K key;
  final V value;
  final int timestamp;

  ConsumerRecord(this.topic, this.partition, this.offset, this.key, this.value,
      this.timestamp);

  TopicPartition get topicPartition => new TopicPartition(topic, partition);
}

// TODO: bad name, figure out better one
class ConsumerRecords<K, V> {
  final Completer<bool> _completer = new Completer();

  /// Collection of consumed records.
  final List<ConsumerRecord<K, V>> records;

  ConsumerRecords(this.records);

  Future<bool> get future => _completer.future;

  void ack() {
    _completer.complete(true);
  }

  bool get isCompleted => _completer.isCompleted;
}
