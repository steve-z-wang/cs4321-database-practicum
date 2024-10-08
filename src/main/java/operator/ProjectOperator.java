package operator;

import common.Tuple;
import java.util.ArrayList;
import java.util.HashMap;
import net.sf.jsqlparser.schema.Column;

public class ProjectOperator extends Operator {

  private final Operator inputOperator;
  private final ArrayList<Integer> outputIndexToInputIndexMap = new ArrayList<>();

  public ProjectOperator(Operator operator, ArrayList<Column> outputSchema) {
    super(null);

    this.inputOperator = operator;
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
  public void reset() {
    this.inputOperator.reset();
  }

  @Override
  public Tuple getNextTuple() {

    // get input
    Tuple inputTuple = inputOperator.getNextTuple();

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
