import 'package:quiver/core.dart';

/// Creates tuple of two values.
Tuple2<T1, T2> tuple2<T1, T2>(T1 $1, T2 $2) {
  return new Tuple2($1, $2);
}

/// Creates tuple of three values.
Tuple3<T1, T2, T3> tuple3<T1, T2, T3>(T1 $1, T2 $2, T3 $3) {
  return new Tuple3($1, $2, $3);
}

class Tuple2<T1, T2> {
  final T1 $1;
  final T2 $2;

  Tuple2(this.$1, this.$2);

  @override
  int get hashCode => hash2($1, $2);

  bool operator ==(o) => o is Tuple3 && o.$1 == $1 && o.$2 == $2;

  @override
  toString() => "[${$1}, ${$2}]";
}

class Tuple3<T1, T2, T3> {
  final T1 $1;
  final T2 $2;
  final T3 $3;

  Tuple3(this.$1, this.$2, this.$3);

  @override
  int get hashCode => hash3($1, $2, $3);

  bool operator ==(o) => o is Tuple3 && o.$1 == $1 && o.$2 == $2 && o.$3 == $3;

  @override
  toString() => "[${$1}, ${$2}, ${$3}]";
}
