part of kafka;

class KafkaMessageStream extends Stream<Message> {
  final KafkaClient client;
  final KafkaConsumer consumer;

  Stream<Message> _stream;
  StreamSubscription<Message> _subscription;
  StreamController<Message> _controller;

  KafkaMessageStream(this.client, this.consumer) {
    _controller = new StreamController<Message>();
    _stream = _controller.stream;
  }

  @override
  StreamSubscription<Message> listen(void onData(Message event),
      {Function onError, void onDone(), bool cancelOnError}) {
    _controller.stream
        .listen(onData, onError: onError, cancelOnError: cancelOnError);
  }

  void _onListen() {
    _subscription =
        _stream.listen(_onData, onError: _controller.addError, onDone: _onDone);
  }

  void _onCancel() {
    _subscription.cancel();
    _subscription = null;
  }

  void _onPause() {
    _subscription.pause();
  }

  void _onResume() {
    _subscription.resume();
  }

  void _onData(Message input) {
    // splits.forEach(_controller.add);
  }

  void _onDone() {
    // if (!_remainder.isEmpty) _controller.add(_remainder);
    _controller.close();
  }
}
