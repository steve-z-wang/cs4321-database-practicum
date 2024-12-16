package physicaloperator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import model.Tuple;
import net.sf.jsqlparser.schema.Column;

public class ProjectOperator extends PhysicalOperator {

  private final PhysicalOperator childOperator;
  private final ArrayList<Integer> outputIndexToInputIndexMap = new ArrayList<>();

  public ProjectOperator(PhysicalOperator operator, ArrayList<Column> outputSchema) {
    super(null);

    this.childOperator = operator;
    ArrayList<Column> inputSchema = operator.getOutputSchema();

    // Set output schema
    this.outputSchema = new ArrayList<>(outputSchema);

    // create a mapping from input schema to output schema
    HashMap<String, Integer> columnNameToInputIndexMap = new HashMap<>();
    for (int i = 0; i < inputSchema.size(); i++) {
      String key = inputSchema.get(i).getFullyQualifiedName(true);
      columnNameToInputIndexMap.put(key, i);
    }

    // create the mapping
    for (Column column : this.outputSchema) {
      String key = column.getFullyQualifiedName(true);
      int index = columnNameToInputIndexMap.get(key);
      this.outputIndexToInputIndexMap.add(index);
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("Project[ ");
    for (Column c : outputSchema) {
      sb.append(c.getFullyQualifiedName(true)).append(", ");
    }
    if (!outputSchema.isEmpty()) {
      sb.setLength(sb.length() - 2);
    }
    sb.append("]");
    List<String> subplan = List.of(childOperator.toString().split("\n"));
    for (String s : subplan) {
      sb.append("\n");
      sb.append("-").append(s);
    }
    return sb.toString();
  }

  @Override
  public void reset() {
    this.childOperator.reset();
  }

  @Override
  public Tuple getNextTuple() {

    // get input
    Tuple inputTuple = childOperator.getNextTuple();

    if (inputTuple == null) {
      return null;
    }

    // create ouput with mapping
    ArrayList<Integer> outputList = new ArrayList<>();
    for (Integer index : this.outputIndexToInputIndexMap) {
      outputList.add(inputTuple.getElementAtIndex(index));
    }

    // return as tuple
    return new Tuple(outputList);
  }
}
