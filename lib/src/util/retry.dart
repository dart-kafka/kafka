import 'dart:async';

Future<T> retryAsync<T>(Future<T> func(), int retries, int delayInMsecs,
    {bool errorCondition(error)}) {
  return func().catchError((error) {
    if (retries == 0) {
      return new Future.error(error);
    } else {
      if (errorCondition is Function && !errorCondition(error)) {
        return new Future.error(error);
      }
      return new Future.delayed(new Duration(milliseconds: delayInMsecs),
          () => retryAsync(func, retries - 1, delayInMsecs));
    }
  });
}
