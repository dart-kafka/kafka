import 'dart:convert';

abstract class Serializer<T> {
  List<int> serialize(T data);
}

/// Serializer for `String` objects. Defaults to UTF8 encoding.
class StringSerializer implements Serializer<String> {
  @override
  List<int> serialize(String data) => UTF8.encode(data);
}

abstract class Deserializer<T> {
  T deserialize(List<int> data);
}

/// Deserializer for `String` objects. Defatuls to UTF8 encoding.
class StringDeserializer implements Deserializer<String> {
  @override
  String deserialize(List<int> data) => UTF8.decode(data);
}
