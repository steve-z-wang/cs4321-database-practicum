package physicaloperator;

import common.Tuple;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;

public class SortOperator extends Operator {
  private final Operator inputOperator;
  private final ArrayList<Tuple> internalBuffer;
  private final List<OrderByElement> orderByElements;
  private int curIndex;

  public SortOperator(Operator operator, List<OrderByElement> orderByElements) {
    super(operator.getOutputSchema());

    this.inputOperator = operator;
    this.orderByElements = orderByElements;

    // Create internal Buffer
    this.internalBuffer = new ArrayList<>();

    loadAndSortBuffer();
  }

  @Override
  public void reset() {
    this.internalBuffer.clear();
    this.inputOperator.reset();
    loadAndSortBuffer();
  }

  @Override
  public Tuple getNextTuple() {

    if (this.curIndex < this.internalBuffer.size()) {
      return this.internalBuffer.get(curIndex++);
    }

    return null;
  }

  private void loadAndSortBuffer() {
    Tuple tuple;
    while ((tuple = this.inputOperator.getNextTuple()) != null) {
      this.internalBuffer.add(tuple);
    }

    // Sort the buffer based on the specified order-by elements
    this.internalBuffer.sort(
        new TupleComparator(this.orderByElements, this.inputOperator.getOutputSchema()));
    this.curIndex = 0;
  }

  private class TupleComparator implements Comparator<Tuple> {

    private final ArrayList<Integer> orderByIndexToTupleIndexMap;

    public TupleComparator(List<OrderByElement> orderByElements, List<Column> tupleSchema) {

      // Map to store the index of each column in the tuple schema
      HashMap<String, Integer> columnNameToTupleIndexMap = new HashMap<>();
      for (int i = 0; i < tupleSchema.size(); i++) {
        Column column = tupleSchema.get(i);
        String key = column.getFullyQualifiedName(true);
        columnNameToTupleIndexMap.put(key, i);
      }

      // List to store the indices of columns to sort by, in the order specified
      this.orderByIndexToTupleIndexMap = new ArrayList<>();
      for (OrderByElement orderByElement : orderByElements) {
        Column column = (Column) orderByElement.getExpression();
        String key = column.getFullyQualifiedName(true);
        int tupleIndex = columnNameToTupleIndexMap.get(key);
        this.orderByIndexToTupleIndexMap.add(tupleIndex);
      }
    }

    @Override
    public int compare(Tuple t1, Tuple t2) {
      for (Integer tupleIndex : this.orderByIndexToTupleIndexMap) {

        Integer value1 = t1.getElementAtIndex(tupleIndex);
        Integer value2 = t2.getElementAtIndex(tupleIndex);

        int comparision = value1.compareTo(value2);
        if (comparision != 0) {
          return comparision;
        }
      }

      return 0;
    }
  }
}
