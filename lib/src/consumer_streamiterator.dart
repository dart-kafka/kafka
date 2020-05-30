import 'dart:async';
import 'common.dart';
import 'consumer.dart';
import 'consumer_offset_api.dart';

/**
 * Extended version of built-in [StreamIterator] implementation.
 *
 * Pauses the stream between calls to [moveNext].
 */
class ConsumerStreamIterator<K, V>
    implements StreamIterator<ConsumerRecords<K, V>> {
  // The stream iterator is always in one of four states.
  // The value of the [_stateData] field depends on the state.
  //
  // When `_subscription == null` and `_stateData != null`:
  // The stream iterator has been created, but [moveNext] has not been called
  // yet. The [_stateData] field contains the stream to listen to on the first
  // call to [moveNext] and [current] returns `null`.
  //
  // When `_subscription != null` and `!_isPaused`:
  // The user has called [moveNext] and the iterator is waiting for the next
  // event. The [_stateData] field contains the [_Future] returned by the
  // [_moveNext] call and [current] returns `null.`
  //
  // When `_subscription != null` and `_isPaused`:
  // The most recent call to [moveNext] has completed with a `true` value
  // and [current] provides the value of the data event.
  // The [_stateData] field contains the [current] value.
  //
  // When `_subscription == null` and `_stateData == null`:
  // The stream has completed or been canceled using [cancel].
  // The stream completes on either a done event or an error event.
  // The last call to [moveNext] has completed with `false` and [current]
  // returns `null`.

  /// Subscription being listened to.
  ///
  /// Set to `null` when the stream subscription is done or canceled.
  StreamSubscription<ConsumerRecords<K, V>> _subscription;

  /// Data value depending on the current state.
  ///
  /// Before first call to [moveNext]: The stream to listen to.
  ///
  /// After calling [moveNext] but before the returned future completes:
  /// The returned future.
  ///
  /// After calling [moveNext] and the returned future has completed
  /// with `true`: The value of [current].
  ///
  /// After calling [moveNext] and the returned future has completed
  /// with `false`, or after calling [cancel]: `null`.
  Object _stateData;

  /// Whether the iterator is between calls to `moveNext`.
  /// This will usually cause the [_subscription] to be paused, but as an
  /// optimization, we only pause after the [moveNext] future has been
  /// completed.
  bool _isPaused = false;

  /// Stores accumulated consumer offsets for each topic-partition.
  ///
  /// Offsets are aggregated so that only latest consumed offset is stored.
  /// These offsets are used by [Consumer] to commit to the server.
  final Map<TopicPartition, ConsumerOffset> _offsets = new Map();

  ConsumerStreamIterator(final Stream<ConsumerRecords<K, V>> stream)
      : _stateData = stream;

  List<ConsumerOffset> get offsets => _offsets.values.toList(growable: false);

  /// Removes all accumulated offsets.
  ///
  /// This method is called by [Consumer] after successful commit of current
  /// offsets.
  void clearOffsets() {
    _offsets.clear();
  }

  void _updateOffsets(ConsumerRecords<K, V> records) {
    for (var record in records.records) {
      var partition = record.topicPartition;
      // TODO: handle metadata?
      _offsets[partition] =
          new ConsumerOffset(record.topic, record.partition, record.offset, '');
    }
  }

  ConsumerRecords<K, V> get current {
    if (_subscription != null && _isPaused) {
      return _stateData as ConsumerRecords<K, V>;
    }
    return null;
  }

  /// Attaches new stream to this iterator.
  ///
  /// [Consumer] calls this method in case of a rebalance event.
  /// This cancels active subscription (if exists) so that no new events can be
  /// delivered to the listener from the previous stream.
  /// Subscription to the new [stream] is created immediately and current state
  /// of the listener is preserved.
  void attachStream(Stream<ConsumerRecords<K, V>> stream) {
    Completer<bool> completer;
    Object records;
    if (_subscription != null) {
      _subscription.cancel();
      _subscription = null;
      if (!_isPaused) {
        // User  waits for `moveNext` to complete.
        completer = _stateData as Completer<bool>;
      } else {
        records = _stateData;
      }
    }
    // During rebalance offset commits are not accepted by the server and result
    // in RebalanceInProgress error (or UnknownMemberId if rebalance completed).
    clearOffsets();
    _stateData = stream;
    _initializeOrDone();
    // Restore state after initialize.
    if (_isPaused) {
      _subscription.pause();
      _stateData = records;
    } else {
      _subscription.resume();
      _stateData = completer;
    }
  }

  Future<bool> moveNext() {
    if (_subscription != null) {
      if (_isPaused) {
        var records = _stateData as ConsumerRecords<K, V>;
        // Acknowledge this record set. Signals to consumer to resume polling.
        records.ack();

        var completer = new Completer<bool>();
        _stateData = completer;
        _isPaused = false;
        _subscription.resume();
        return completer.future;
      }
      throw new StateError("Already waiting for next.");
    }
    return _initializeOrDone();
  }

  /// Called if there is no active subscription when [moveNext] is called.
  ///
  /// Either starts listening on the stream if this is the first call to
  /// [moveNext], or returns a `false` future because the stream has already
  /// ended.
  Future<bool> _initializeOrDone() {
    assert(_subscription == null);
    var stateData = _stateData;
    if (stateData != null) {
      Stream<ConsumerRecords<K, V>> stream =
          stateData as Stream<ConsumerRecords<K, V>>;
      _subscription = stream.listen(_onData,
          onError: _onError, onDone: _onDone, cancelOnError: true);
      var completer = new Completer<bool>();
      _stateData = completer;
      return completer.future;
    }
    return new Future<bool>.value(false);
  }

  Future cancel() {
    StreamSubscription<ConsumerRecords<K, V>> subscription = _subscription;
    Object stateData = _stateData;
    _stateData = null;
    if (subscription != null) {
      _subscription = null;
      if (!_isPaused) {
        Completer<bool> completer = stateData as Completer<bool>;
        completer.complete(false);
      }
      return subscription.cancel();
    }
    return new Future.value(null);
  }

  void _onData(ConsumerRecords<K, V> data) {
    assert(_subscription != null && !_isPaused);
    Completer<bool> moveNextFuture = _stateData as Completer<bool>;
    _stateData = data;
    _isPaused = true;
    _updateOffsets(data);
    moveNextFuture.complete(true);
    if (_subscription != null && _isPaused) _subscription.pause();
  }

  void _onError(Object error, [StackTrace stackTrace]) {
    assert(_subscription != null && !_isPaused);
    Completer<bool> moveNextFuture = _stateData as Completer<bool>;
    _subscription = null;
    _stateData = null;
    moveNextFuture.completeError(error, stackTrace);
  }

  void _onDone() {
    assert(_subscription != null && !_isPaused);
    Completer<bool> moveNextFuture = _stateData as Completer<bool>;
    _subscription = null;
    _stateData = null;
    moveNextFuture.complete(false);
  }
}
