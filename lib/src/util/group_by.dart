Map<K, List<V>> groupBy<K, V>(List<V> list, K func(V element)) {
  var grouped = new Map<K, List<V>>();
  for (var element in list) {
    K key = func(element);
    if (!grouped.containsKey(key)) {
      grouped[key] = new List<V>();
    }
    grouped[key].add(element);
  }

  return grouped;
}
