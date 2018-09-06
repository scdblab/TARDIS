package mongodb;

import java.util.*;

public class DecreasingComparator<T> implements Comparator<T> {

  @Override
  public int compare(T o1, T o2) {
    if (o2 instanceof Integer) {
      return ((Integer)o2).intValue() - ((Integer)o2).intValue();
    } else {
      return 0;
    }
  }
}
