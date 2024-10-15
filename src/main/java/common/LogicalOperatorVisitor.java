package common;

import logicaloperator.*;

public interface LogicalOperatorVisitor {
    void visit(LogicalScan logicalScan);
    void visit(LogicalSelect logicalSelect);
    void visit(LogicalJoin logicalJoin);
    void visit(LogicalSort logicalSort);
    void visit(LogicalDistinct logicalDistinct);
    void visit(LogicalProject logicalProject);
}
