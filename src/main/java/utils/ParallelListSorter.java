package utils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

public class ParallelListSorter {
  public static <K extends Comparable<K>, V> void sortParallelLists(List<K> keys, List<V> values) {
    if (keys == null || values == null || keys.size() != values.size()) {
      throw new IllegalArgumentException("Lists must be non-null and of equal size");
    }

    // Create indices array
    List<Integer> indices =
        IntStream.range(0, keys.size())
            .boxed()
            .sorted((i, j) -> keys.get(i).compareTo(keys.get(j)))
            .toList();

    // Create temporary lists to store sorted results
    List<K> sortedKeys = new ArrayList<>(keys.size());
    List<V> sortedValues = new ArrayList<>(values.size());

    // Fill temporary lists with null to ensure capacity
    for (int i = 0; i < keys.size(); i++) {
      sortedKeys.add(null);
      sortedValues.add(null);
    }

    // Rearrange elements based on sorted indices
    for (int i = 0; i < indices.size(); i++) {
      sortedKeys.set(i, keys.get(indices.get(i)));
      sortedValues.set(i, values.get(indices.get(i)));
    }

    // Copy back to original lists
    for (int i = 0; i < keys.size(); i++) {
      keys.set(i, sortedKeys.get(i));
      values.set(i, sortedValues.get(i));
    }
  }
}
