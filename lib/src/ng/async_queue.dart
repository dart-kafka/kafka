import 'dart:async';

typedef Future AsyncCallback(value);

class AsyncQueue {
  Future queue;
  void add(AsyncCallback callback) {
    queue = queue.then(callback);
  }
}
