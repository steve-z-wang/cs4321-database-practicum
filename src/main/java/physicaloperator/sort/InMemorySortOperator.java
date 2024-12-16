package physicaloperator.sort;

import java.util.ArrayList;
import java.util.List;
import model.Tuple;
import net.sf.jsqlparser.statement.select.OrderByElement;
import physicaloperator.PhysicalOperator;
import utils.SortTupleComparator;

public class InMemorySortOperator extends PhysicalOperator {
  private final PhysicalOperator childOperator;
  private final ArrayList<Tuple> internalBuffer;
  private final List<OrderByElement> orderByElements;
  private int curIndex;

  public InMemorySortOperator(PhysicalOperator operator, List<OrderByElement> orderByElements) {
    super(operator.getOutputSchema());

    this.childOperator = operator;
    this.orderByElements = orderByElements;

    // Create internal Buffer
    this.internalBuffer = new ArrayList<>();

    loadAndSortBuffer();
  }

  @Override
  public void reset() {
    this.internalBuffer.clear();
    this.childOperator.reset();
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
    while ((tuple = this.childOperator.getNextTuple()) != null) {
      this.internalBuffer.add(tuple);
    }

    // Sort the buffer based on the specified order-by elements
    this.internalBuffer.sort(
        new SortTupleComparator(this.childOperator.getOutputSchema(), this.orderByElements));
    this.curIndex = 0;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("-InMomorySort[");
    for (OrderByElement element : orderByElements) {
      sb.append(element.getExpression().toString());
      sb.append(", ");
    }
    sb.append("]");

    List<String> subplan = List.of(this.childOperator.toString().split("\n"));

    for (String s : subplan) {
      sb.append("\n");
      sb.append("-").append(s);
    }
    return sb.toString();
  }
}
