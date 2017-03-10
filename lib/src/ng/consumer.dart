import 'dart:async';

import 'package:logging/logging.dart';

import '../util/tuple.dart';
import '../util/group_by.dart';
import 'common.dart';
import 'errors.dart';
import 'consumer_group.dart';
import 'fetch_api.dart';
import 'consumer_offset_api.dart';
import 'group_membership_api.dart';
import 'metadata.dart';
import 'offset_master.dart';
import 'serialization.dart';
import 'session.dart';
import 'async.dart';

final Logger _logger = new Logger('Consumer');

/// Consumes messages from Kafka cluster.
///
/// Consumer interacts with the server to allow multiple members of the same group
/// load balance consumption by distributing topics and partitions evenly across
/// all members.
///
/// ## Usage example
///
///     TODO: write an example
abstract class Consumer<K, V> {
  /// The consumer group name.
  String get group;

  /// Starts polling Kafka servers for new messages.
  StreamIterator<ConsumerRecords<K, V>> poll();

  /// Subscribes to [topics] as a member of consumer [group].
  ///
  /// Subscribe triggers rebalance of all currently active members of the same
  /// consumer grouop.
  Future subscribe(List<String> topics);

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
  static const int DEFAULT_MAX_WAIT_TIME = 1000;
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

  /// Returns `true` if initial subscription is in progress.
  ///
  /// This is not used by following resubscriptions since it's handled by
  /// the state machine.
  bool _isSubscribing = false;

  /// List of topics to subscribe to when joining the group.
  ///
  /// Set by initial call to [subscribe] and used during initial
  /// subscribe and possible resubscriptions.
  List<String> _topics;

  StreamController<ConsumerRecords<K, V>> _streamController;
  ConsumerStreamIterator<K, V> _streamIterator;
  Completer _resumeCompleter;
  Future get resumeFuture => _resumeCompleter.future;

  /// Whether user canceled stream subscription.
  ///
  /// This triggers shutdown of polling phase.
  bool _isCanceled = false;

  /// Current position of this consumer in subscribed topic-partitions.
  ///
  /// Different from current committed offsets of this consumer.
  Map<TopicPartition, ConsumerOffset> _partitionOffsets;

  /// Whether resubscription is required due to rebalance event received from
  /// the server.
  bool _resubscriptionNeeded = false;

  @override
  Future subscribe(List<String> topics) {
    assert(!_isSubscribing, 'Subscription already in progress.');
    assert(_subscription == null, 'Already subscribed.');
    _isSubscribing = true;
    _topics = new List.from(topics, growable: false);
    return _resubscribeState().whenComplete(() {
      _isSubscribing = false;
    });
  }

  /// State of this consumer during (re)subscription.
  ///
  /// Consumer enters this state initially on [subscribe] call and may
  /// re-enter this state in case of a rebalance event triggerred by the server.
  Future _resubscribeState() {
    _logger
        .info('Subscribing to topics ${_topics} as a member of group $group');
    var protocols = [new GroupProtocol.roundrobin(0, _topics.toSet())];
    return _group.join(30000, '', 'consumer', protocols).then((result) {
      // TODO: resume heartbeat timer.
      _subscription = result;
      _resubscriptionNeeded = false;
      _logger.info('Subscription result: ${subscription}.');
      // If polling already started switch to polling state
      if (_streamController != null) {
        _activeState = _pollState;
      }
    });
  }

  @override
  StreamIterator<ConsumerRecords<K, V>> poll() {
    assert(subscription != null,
        'No active subscription. Must first call subscribe().');
    assert(_streamController == null, 'Already polling.');

    _streamController = new StreamController<ConsumerRecords>(
        onPause: onPause, onResume: onResume, onCancel: onCancel);
    _streamIterator =
        new ConsumerStreamIterator<K, V>(_streamController.stream);
    _activeState = _pollState;

    _run().whenComplete(() {
      // TODO: ensure cleanup here, e.g. shutdown heartbeats
      var closeFuture = _streamController.close();
      _streamController = null;
      _streamIterator = null;
      return closeFuture;
    });
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

  void onPause() {
    assert(_resumeCompleter == null);
    _resumeCompleter = new Completer();
  }

  void onResume() {
    assert(_resumeCompleter is Completer && !_resumeCompleter.isCompleted);
    _resumeCompleter.complete();
    _resumeCompleter = null;
  }

  void onCancel() {
    _isCanceled = true;
  }

  /// Poll state of this consumer's state machine.
  ///
  /// Consumer enters this state when [poll] is executed and stays in this state
  /// until:
  ///
  /// - user cancels the poll, results in "clean exit".
  /// - a rebalance error received from the server, which transitions state machine to
  ///   the [_resubscribeState].
  /// - an unhandled error occurs, closes underlying stream and forwards the error to the
  ///   user.
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
      if (_resubscriptionNeeded) _activeState = _resubscribeState;
      // reset offsets and addQueue(?)
      _partitionOffsets = null;
      // Clear any accumulated uncommitted offsets in the iterator to avoid
      // any additional errors triggerred by OffsetCommit requests.
      // When rejoined we'll start consuming from latest committed offsets
      // (at-least-once delivery).
      _streamIterator.clearOffsets();
    });
  }

  bool isRebalanceError(error) =>
      error is RebalanceInProgressError || error is UnknownMemberIdError;

  /// Internal polling method.
  Future _poll() async {
    var offsetList = await _fetchOffsets(subscription);
    _partitionOffsets = new Map.fromIterable(offsetList,
        key: (ConsumerOffset offset) => offset.topicPartition);
    _logger.fine('Polling started from following offsets: ${offsetList}');

    while (true) {
      if (_resubscriptionNeeded) {
        _logger.info('Stoping poll for resubscription.');
        break;
      }
      if (_isCanceled) {
        _logger.fine('Stream subscription was canceled. Stoping poll.');
        break;
      }

      // TODO: Implement a more efficient polling algorithm.
      Map<Broker, FetchRequest> requests = await _buildRequests(
          _partitionOffsets.values.toList(growable: false));
      var futures = requests.keys.map((broker) {
        return session
            .send(requests[broker], broker.host, broker.port)
            .then((response) {
          _addQueue = _addQueue.then((_) => add(response));
          return _addQueue;
        });
      });
      // Depending on configuration this can be very inefficient.
      // It always waits for all responses before returning to the user.
      await Future.wait(futures);
    }
  }

  Future _addQueue = new Future.value();

  /// Adds consumed messages to the stream.
  Future add(FetchResponse response) async {
    if (_isCanceled) return;
    if (_streamController.isPaused) {
      _logger.fine('Will not add to the stream while it\'s paused. Waiting.');
      await resumeFuture;
    }

    /// Iterator resumes subscription when it starts waiting for the next event
    /// which means previous event is fully processed at this point. However
    /// we might be in a state which requires resubscription, for instance,
    /// when `commit()` completed with `RebalanceInProgressError`.
    /// In this case we should not add current record set to the stream and
    /// instead just skip this step here to avoid having en-route records
    /// between two different subscriptions, this can lead to undesirable
    /// offset commits by the client.
    if (_resubscriptionNeeded) return;

    // There was no errors, subscription is active, go ahead and add this
    // record set to the stream.
    var records = fetchResultsToRecords(response.results);
    if (records.isEmpty) return;
    updateOffsets(records);
    _streamController.add(new ConsumerRecords(records));
  }

  void updateOffsets(List<ConsumerRecord> records) {
    for (var rec in records) {
      var partition = new TopicPartition(rec.topic, rec.partition);
      _partitionOffsets[partition] =
          new ConsumerOffset(rec.topic, rec.partition, rec.offset, '');
    }
  }

  List<ConsumerRecord<K, V>> fetchResultsToRecords(List<FetchResult> results) {
    return results.expand((result) {
      return result.messages.keys.map((offset) {
        var key = keyDeserializer.deserialize(result.messages[offset].key);
        var value =
            valueDeserializer.deserialize(result.messages[offset].value);
        return new ConsumerRecord<K, V>(
            result.topic, result.partition, offset, key, value);
      });
    }).toList(growable: false);
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

  Future<Map<Broker, FetchRequest>> _buildRequests(
      List<ConsumerOffset> offsets) async {
    Map<Broker, List<TopicPartition>> brokers =
        await _fetchPartitionLeaders(subscription.assignment.topics);

    // Add 1 to current offset since current offset indicates already
    // processed message and we don't want to consume it again.
    List<Tuple3<Broker, TopicPartition, int>> data = offsets
        .map((o) =>
            tuple3(brokers[o.topicPartition], o.topicPartition, o.offset + 1))
        .toList(growable: false);

    Map<Broker, FetchRequest> requests = new Map();
    for (var item in data) {
      requests.putIfAbsent(item.$1,
          () => new FetchRequest(DEFAULT_MAX_WAIT_TIME, DEFAULT_MIN_BYTES));
      requests[item.$1].add(item.$2, new FetchData(item.$3, requestMaxBytes));
    }
    return requests;
  }

  Future<Map<Broker, List<TopicPartition>>> _partitionLeaders;
  Future<Map<Broker, List<TopicPartition>>> _fetchPartitionLeaders(
      List<String> topics) {
    if (_partitionLeaders == null) {
      _partitionLeaders = new Future(() async {
        var meta = new Metadata(session);
        var topicsMeta = await meta.fetchTopics(topics);
        return groupBy(topicsMeta.topicPartitions, (_) {
          var leaderId = topicsMeta[_.topic].partitions[_.partition].leader;
          return topicsMeta.brokers[leaderId];
        });
      });
    }
    return _partitionLeaders;
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

  ConsumerRecord(this.topic, this.partition, this.offset, this.key, this.value);
}

class ConsumerRecords<K, V> {
  /// List of consumed records.
  final List<ConsumerRecord<K, V>> records;

  ConsumerRecords(this.records);
}
