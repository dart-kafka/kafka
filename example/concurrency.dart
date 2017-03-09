import 'dart:async';

var controller = new StreamController();
var iterator = new StreamIterator(controller.stream);

var rebalanceNeeded = false;

Future main() async {
  var queue = build();
  Future.wait(queue);
  int i = 0;
  while (await iterator.moveNext()) {
    print(iterator.current);
    resume = new Future.delayed(new Duration(seconds: 1), () {
      print('waited another second: ${i++}');
    });
  }
}

Future resume = new Future.delayed(new Duration(seconds: 2), () {
  print('waited for 2 seconds');
});

List<Future> build() {
  var f1 = new Future.delayed(new Duration(seconds: 1), () => 'value1')
      .then((value) {
    print('request completed for $value');
    _addFuture = _addFuture.then((_) => add(value));
  });
  var f2 = new Future.delayed(new Duration(seconds: 1), () => 'value2')
      .then((value) {
    print('request completed for $value');
    _addFuture = _addFuture.then((_) => add(value));
  });
  var f3 = new Future.delayed(new Duration(seconds: 1), () => 'value3')
      .then((value) {
    print('request completed for $value');
    _addFuture = _addFuture.then((_) => add(value));
  });

  return [f1, f2, f3];
}

Future _addFuture = new Future.value();

Future add(value) async {
  if (value == null) return;

  print('waiting for resume: $value');
  await resume;
  if (rebalanceNeeded) {
    controller.close();
    return;
  }

  controller.add(value);
  if (value == 'value3') controller.close();
}
