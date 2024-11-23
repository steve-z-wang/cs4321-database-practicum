package builder;

import jdk.jshell.spi.ExecutionControl;
import logicaloperator.LogicalOperator;
import net.sf.jsqlparser.statement.Statement;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import physicaloperator.base.PhysicalOperator;

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
    logger.debug("Created logical plan: {}", logicalPlan);

    logger.info("Building physical plan for query: {}", stmt);
    logicalPlan.accept(physicalPlanBuilder);
    return physicalPlanBuilder.getPhysicalPlan();
  }
}
