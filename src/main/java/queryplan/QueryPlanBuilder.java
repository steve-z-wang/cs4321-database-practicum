package queryplan;

import jdk.jshell.spi.ExecutionControl;
import logicaloperator.LogicalJoin;
import logicaloperator.LogicalOperator;
import net.sf.jsqlparser.statement.Statement;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import physicaloperator.PhysicalOperator;

import java.util.List;

/**
 * Class to translate a JSQLParser statement into a relational algebra query plan. For now only
 * works for Statements that are Selects, and specifically PlainSelects. Could implement the visitor
 * pattern on the statement, but doesn't for simplicity as we do not handle nesting or other complex
 * query features.
 *
 * <p>Query plan fixes join order to the order found in the from clause and uses a left deep tree
 * join. Maximally pushes selections on individual relations and evaluates join conditions as early
 * as possible in the join tree. Projections (if any) are not pushed and evaluated in a single
 * projection operator after the last join. Finally, sorting and duplicate elimination are added if
 * needed.
 *
 * <p>For the subset of SQL which is supported as well as assumptions on semantics, see the Project
 * 2 student instructions, Section 2.1
 */
public class QueryPlanBuilder {

  private static final Logger logger = LogManager.getLogger(QueryPlanBuilder.class);

  private final LogicalPlanBuilder logicalPlanBuilder;
  private final PhysicalPlanBuilder physicalPlanBuilder;
  private String logicalSctructure;
  private String physicalSctructure;
  private static String logicalToString(LogicalOperator root) {
    StringBuilder sb = new StringBuilder();
    getlogicalPlan(root, 0, sb);
    return sb.toString();
  }
  private static void getlogicalPlan(LogicalOperator node, int level, StringBuilder sb) {
    if(node == null) return;
    for(int i = 0; i <= level; i++) {
      sb.append("-");
    }
    sb.append(node.toString()).append("\n");
    for(LogicalOperator child : node.getChildren()){
      getlogicalPlan(child, level+1, sb);
    }
  }


  public QueryPlanBuilder() {
    this.logicalPlanBuilder = new LogicalPlanBuilder();
    this.physicalPlanBuilder = new PhysicalPlanBuilder();
  }

  /**
   * Top level method to translate statement to query plan
   *
   * @param stmt statement to be translated
   * @return the root of the query plan
   * @precondition stmt is a Select having a body that is a PlainSelect
   */
  public PhysicalOperator buildPlan(Statement stmt)
      throws ExecutionControl.NotImplementedException {
    LogicalOperator logicalPlan = logicalPlanBuilder.buildPlan(stmt);
//    traverse the logicalPlan tree
    String logical = logicalToString(logicalPlan);
    logger.debug("Created logical plan: {}", logical);
    this.logicalSctructure = logical;
    logger.info("Building physical plan for query: {}", stmt);
    logicalPlan.accept(physicalPlanBuilder);

    PhysicalOperator physicalPlan = physicalPlanBuilder.getPhysicalPlan();
    String physical = physicalPlan.toString();
    this.physicalSctructure = physical;
    logger.debug("Created physical plan: {}", physical);
    return physicalPlanBuilder.getPhysicalPlan();
  }

  public String getLogicalSctructure() {
    return logicalSctructure;
  }
  public String getPhysicalSctructure() {
    return physicalSctructure;
  }
}
