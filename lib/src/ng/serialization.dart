import 'dart:convert';

abstract class Serializer<T> {
  List<int> serialize(T data);
}

/// Serializer for `String` objects. Defaults to UTF8 encoding.
class StringSerializer implements Serializer<String> {
  @override
  List<int> serialize(String data) => UTF8.encode(data);
}
