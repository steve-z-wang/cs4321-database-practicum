package operator;

import common.Tuple;
import java.util.ArrayList;
import java.util.HashMap;
import net.sf.jsqlparser.schema.Column;

public class ProjectOperator extends Operator {

  private final Operator inputOperator;
  private final ArrayList<Integer> inputToOutMapping = new ArrayList<>();

  public ProjectOperator(Operator operator, ArrayList<Column> outputSchema) {
    super(outputSchema);

    this.inputOperator = operator;

    // create a mapping from input schema to output schema

    // get input schema
    ArrayList<Column> inputColumns = this.inputOperator.getOutputSchema();

    // create a hashmap for input schema
    HashMap<String, Integer> inputColumnMap = new HashMap<>();
    for (int i = 0; i < inputColumns.size(); i++) {
      inputColumnMap.put(inputColumns.get(i).toString(), i);
    }

    // create the mapping
    for (Column column : outputSchema) {
      int index = inputColumnMap.get(column.toString());
      this.inputToOutMapping.add(index);
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
    for (Integer index : inputToOutMapping) {
      outputList.add(inputTuple.getElementAtIndex(index));
    }

    // return as tuple
    return new Tuple(outputList);
  }
}
