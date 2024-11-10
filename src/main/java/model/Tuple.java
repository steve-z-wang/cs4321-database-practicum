package model;

import java.util.ArrayList;
import java.util.List;

/**
 * Class to encapsulate functionality about a database tuple. A tuple is an ArrayList of integers.
 */
public class Tuple {

  private final ArrayList<Integer> tupleArray;

  /**
   * Creates a tuple using string representation of the tuple. Delimiter between the columns is a
   * comma.
   *
   * @param s String representation of the tuple.
   */
  public Tuple(String s) {
    tupleArray = new ArrayList<>();
    for (String attribute : s.split(",")) {
      tupleArray.add(Integer.parseInt(attribute));
    }
  }

  /**
   * Creates a tuple using an ArrayList of integers.
   *
   * @param elements ArrayList with elements of the tuple, in order
   */
  public Tuple(List<Integer> elements) {
    tupleArray = new ArrayList<>();
    tupleArray.addAll(elements);
  }

  /**
   * Returns element at index i in the tuple.
   *
   * @param i The index of the element you need.
   * @return Element at index i in the tuple.
   */
  public int getElementAtIndex(int i) {
    return tupleArray.get(i);
  }

  /**
   * Returns a new ArrayList containing all the elements in the tuple.
   *
   * @return ArrayList containing the elements in the tuple.
   */
  public ArrayList<Integer> getAllElements() {
    return new ArrayList<>(tupleArray);
  }

  /**
   * Appends another tuple's elements to this tuple and returns a new tuple
   *
   * @param other the tuple to append
   * @return a new Tuple containing elements from both tuples
   */
  public Tuple append(Tuple other) {
    ArrayList<Integer> combinedElements = new ArrayList<>(this.tupleArray);
    combinedElements.addAll(other.getAllElements());
    return new Tuple(combinedElements);
  }

  /**
   * Returns a string representation of the tuple.
   *
   * @return string representation of the tuple, with attributes separated by commas.
   */
  @Override
  public String toString() {
    if (tupleArray.isEmpty()) {
      return "";
    }
    StringBuilder stringRepresentation = new StringBuilder();
    for (int i = 0; i < tupleArray.size() - 1; i++) {
      stringRepresentation.append(tupleArray.get(i)).append(",");
    }
    stringRepresentation.append(tupleArray.get(tupleArray.size() - 1));
    return stringRepresentation.toString();
  }

  /**
   * Checks if two tuples are equal by comparing their elements.
   *
   * @param obj The object to compare with
   * @return True if the two tuples have the same elements in the same order
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    Tuple other = (Tuple) obj;
    return tupleArray.equals(other.tupleArray);
  }

  /**
   * Generates a hash code for the tuple based on its elements.
   *
   * @return hash code for the tuple
   */
  @Override
  public int hashCode() {
    return tupleArray.hashCode();
  }
}
