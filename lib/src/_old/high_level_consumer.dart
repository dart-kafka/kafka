part of kafka;

typedef Future OnOffsetOutOfRange(
    OffsetOutOfRangeError error, group, membership);

class HighLevelConsumer {
  static final Logger _logger = new Logger('HighLevelConsumer');

  /// Instance of KafkaSession.
  final KafkaSession session;

  /// Topics to consume.
  final Set<String> topics;

  final group;

  StreamController<MessageEnvelope> _controller;

  var _membership;
  dynamic get membership => _membership;

  Completer _doneCompleter = new Completer();

  OnOffsetOutOfRange onOffsetOutOfRange;

  HighLevelConsumer(this.session, this.topics, this.group) {
    _controller = new StreamController<MessageEnvelope>(
        onListen: _onListen,
        onPause: _onPause,
        onResume: _onResume,
        onCancel: _onCancel);
  }

  void _onListen() {
    _logger.info('Listener added, starting consumer.');
    _loop();
  }

  Future _join([previousMembership]) {
    var protocols = [new GroupProtocol.roundrobin(0, topics)];
    var memberId =
        (previousMembership != null) ? previousMembership.memberId : '';
    _logger.info(
        '(Re)joining to consumer group ${group.name}. Previous memberId: "${memberId}".');
    return group.join(30000, '', 'consumer', protocols);
  }

  Future _leave(membership) {
    return group.leave(membership).catchError((error) {
      _logger.warning('Received ${error} on attempt to leave group.');
    });
  }

  Future _loop() async {
    try {
      var previousMembership;
      var _rejoinNeeded = true;
      while (_controller.hasListener) {
        try {
          if (_controller.isPaused) {
            await _resumeFuture;
            continue; // skip to next cycle to check if we still have a listener.
          }

          if (_rejoinNeeded) {
            _membership = await _join(previousMembership);
            _logger.info('Joined consumer group. '
                'MemberId: "${_membership.memberId}". GenerationId: ${_membership.generationId}. '
                'Assignment: ${_membership.assignment.partitions}');
          } else {
            _logger.info('Re-using existing membership.');
            _membership = previousMembership;
          }
          _rejoinNeeded = false;
          assert(_membership != null);

          await _cycle(_membership);
        } catch (error) {
          _logger.finer('Cycle completed with error $error.');
          if (error is UnknownMemberIdError ||
              error is RebalanceInProgressError ||
              error is IllegalGenerationError) {
            _logger.warning('Received ${error}, going to rejoin the group.');

            _rejoinNeeded = true;
          } else if (error is OffsetOutOfRangeError &&
              onOffsetOutOfRange != null) {
            await onOffsetOutOfRange(error, group, _membership);
          } else {
            _controller.addError(error);
            // _controller.close();
            // _doneCompleter.complete();
            break;
          }
        } finally {
          previousMembership = _membership;
          _membership = null;
        }
      }
    } finally {
      await _leave(_membership);
      _controller.close();
      _doneCompleter.complete();
    }
  }

  Future _cycle(membership) {
    var completer = new Completer();

    var workers = _HLWorker.build(membership, _controller, session, group);
    workers.then((workers) {
      var remaining = workers.length;
      var futures = workers.map((w) => w.run());
      var capturedError;
      for (var f in futures) {
        f.catchError((error) {
          // In case of error we signal all workers to cancel, wait for them to
          // finish their operations and then throw the error.
          _logger.warning('Captured ${error} from worker.');
          if (capturedError == null) {
            capturedError = error;
            workers.forEach((_) => _.cancel());
          }
        }).whenComplete(() {
          remaining--;
          if (remaining == 0) {
            if (capturedError != null) {
              _logger.finer('Finalizing cycle with error ${capturedError}.');
              completer.completeError(capturedError);
            } else {
              _logger.finer('Finalizing cycle (no error).');
              completer.complete();
            }
          }
        });
      }
    }, onError: (error) {
      completer.completeError(error);
    });

    return completer.future;
  }

  Completer _resumeCompleter;
  Future get _resumeFuture => _resumeCompleter?.future;

  void _onPause() {
    // no-op, workers check for controller status themselves
    _resumeCompleter = new Completer();
  }

  void _onResume() {
    // no-op, workers check for controller status themselves
    _resumeCompleter.complete();
    _resumeCompleter = null;
  }

  dynamic _onCancel() {
    // Make sure to resume the loop if paused.
    _resumeCompleter?.complete();
    _resumeCompleter = null;
    return _doneCompleter.future;
  }

  Stream<MessageEnvelope> get stream => _controller.stream;
}

/// High-level consumer worker
class _HLWorker {
  final StreamController<MessageEnvelope> controller;
  final Map<String, Set<int>> topics;
  final KafkaSession session;
  final Broker broker;
  final membership;
  final group;
  final bool heartbeatWorker;

  bool _isCanceled = false;

  _HLWorker(this.controller, this.topics, this.session, this.broker,
      this.membership, this.group,
      {this.heartbeatWorker: false});

  static Future<Iterable<_HLWorker>> build(
      membership,
      StreamController<MessageEnvelope> controller,
      KafkaSession session,
      group) async {
    var workers = new List<_HLWorker>();

    var topics = membership.assignment.partitions.keys.toSet();
    if (topics.isEmpty) {
      // Worker just to send pings to the server.
      HighLevelConsumer._logger.warning('Received empty assignment.');
      workers.add(new _HLWorker(
          controller, null, session, null, membership, group,
          heartbeatWorker: true));
    } else {
      var meta = await session.getMetadata(topics);
      var topicsByBroker = new Map<Broker, Map<String, Set<int>>>();
      for (var topic in meta.topics) {
        for (var partition in topic.partitions) {
          if (!membership.assignment.partitions[topic.topicName]
              .contains(partition.partitionId)) continue;
          var broker = meta.getBroker(partition.leader);
          topicsByBroker.putIfAbsent(broker, () => new Map());
          topicsByBroker[broker]
              .putIfAbsent(topic.topicName, () => new Set<int>());
          topicsByBroker[broker][topic.topicName].add(partition.partitionId);
        }
      }

      for (var broker in topicsByBroker.keys) {
        workers.add(new _HLWorker(controller, topicsByBroker[broker], session,
            broker, membership, group));
      }
      // And heartbeat worker
      workers.add(new _HLWorker(
          controller, null, session, null, membership, group,
          heartbeatWorker: true));
    }

    return workers;
  }

  bool get _shouldContinue =>
      _isCanceled == false &&
      (controller.hasListener && !controller.isPaused && !controller.isClosed);

  Future run() {
    if (heartbeatWorker) {
      return _heartbeat();
    } else {
      return _consume();
    }
  }

  Timer _heartbeatTimer;
  Completer _heartbeatCompleter;

  Future _heartbeat() {
    if (_heartbeatCompleter == null) {
      _heartbeatCompleter = new Completer();
      _heartbeatTimer = new Timer.periodic(new Duration(seconds: 3), (timer) {
        group.heartbeat(membership).catchError((error) {
          _heartbeatCompleter.completeError(error);
          timer.cancel();
        });
      });
    }

    return _heartbeatCompleter.future;
  }

  Future _consume() async {
    while (_shouldContinue) {
      var request = await _createFetchRequest();

      FetchResponse response = await session.send(broker, request);

      for (var item in response.results) {
        if (!_shouldContinue) break;
        for (var offset in item.messageSet.messages.keys) {
          if (!_shouldContinue) break;
          var message = item.messageSet.messages[offset];
          var envelope = new MessageEnvelope(
              item.topicName, item.partitionId, offset, message);
          controller.add(envelope);
          var result = await envelope.result;
          if (result.status == _ProcessingStatus.commit) {
            var offsets = [
              new ConsumerOffset(item.topicName, item.partitionId, offset,
                  result.commitMetadata)
            ];
            await group.commitOffsets(offsets, subscription: membership);
          }
        }
      }
    }
  }

  cancel() {
    _isCanceled = true;
    if (heartbeatWorker) {
      _heartbeatTimer.cancel();
      if (_heartbeatCompleter is Completer &&
          !_heartbeatCompleter.isCompleted) {
        _heartbeatCompleter.complete();
      }
    }
  }

  Future<FetchRequest> _createFetchRequest() async {
    var offsets = await group.fetchOffsets(topics);
    var maxWaitTime = 100;
    var minBytes = 1;
    var request = new FetchRequest(maxWaitTime, minBytes);
    for (var o in offsets) {
      request.add(o.name, o.id, o.offset + 1);
    }

    return request;
  }
}
