package physicaloperator;

import io.writer.TupleWriter;
import java.io.PrintStream;
import java.util.ArrayList;
import model.Tuple;
import net.sf.jsqlparser.schema.Column;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Abstract class to represent relational operators. Every operator has a reference to an
 * outputSchema which represents the schema of the output tuples from the operator. This is a list
 * of Column objects. Each Column has an embedded Table object with the name and alias (if required)
 * fields set appropriately.
 */
public abstract class PhysicalOperator {

  Logger logger = LogManager.getLogger(PhysicalOperator.class);

  protected ArrayList<Column> outputSchema;

  public PhysicalOperator(ArrayList<Column> outputSchema) {
    this.outputSchema = outputSchema;
  }

  /**
   * Get the output schema of the operator The output schema is a list of Columns, which always
   * contains the table with the alias if exists
   *
   * @return output schema
   */
  public ArrayList<Column> getOutputSchema() {
    return outputSchema;
  }

  /** Resets cursor on the operator to the beginning */
  public abstract void reset();

  /** Used only by sort methods * */
  public void reset(int index) {
    logger.error("Reset with index not implemented for this operator");
  }

  /**
   * Get next tuple from operator
   *
   * @return next Tuple, or null if we are at the end
   */
  public abstract Tuple getNextTuple();

  /**
   * Iterate through output of operator and send it all to the specified printStream)
   *
   * @param printStream stream to receive output, one tuple per line.
   */
  public void dump(PrintStream printStream) {
    Tuple t;
    while ((t = this.getNextTuple()) != null) {
      printStream.println(t);
    }
  }

  /**
   * Iterate through output of operator and send it all to the specified TupleWriter
   *
   * @param writer TupleWriter to receive output
   */
  public void dump(TupleWriter writer) {
    Tuple t;
    while ((t = this.getNextTuple()) != null) {
      writer.writeTuple(t);
    }
    writer.close();
  }
}
