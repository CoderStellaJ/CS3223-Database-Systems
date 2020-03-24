package qp.optimizer;

import qp.operators.*;
import qp.parser.ProcessedQuery;
import java.util.List;

public class SetOperationMergePlan {

    Operator root;
    List<Operator> operators;
    List <ProcessedQuery> queries;

    public SetOperationMergePlan(List<Operator> operators, List <ProcessedQuery> queries) {
        this.operators = operators;
        this.queries = queries;
    }

    public Operator mergeSetOperations() {
        root = operators.get(0);
        for (int i = 0; i < queries.size(); i++) {
            int setOperationType = queries.get(i).getType();
            switch (setOperationType) {
                case 0:
                    break;
                case 1:
                    createUnionOp(root, operators.get(i + 1));
                    break;
                case 2:
                    createIntersectionOp(root, operators.get(i + 1));
                    break;
                default:
                    System.err.println("Invalid Set Operation Type");
                    System.exit(1);
            }
        }

        return this.root;
    }

    private void createUnionOp(Operator leftOp, Operator rightOp) {
        SortedRun leftRun = new SortedRun(leftOp, BufferManager.numBuffer);
        leftRun.setSchema(leftOp.getSchema());
        leftOp = new Distinct(leftRun, OpType.DISTINCT);
        leftOp.setSchema(leftRun.getSchema());
        SortedRun rightRun = new SortedRun(rightOp, BufferManager.numBuffer);
        rightRun.setSchema(rightOp.getSchema());
        rightOp = new Distinct(rightRun, OpType.DISTINCT);
        rightOp.setSchema(rightRun.getSchema());
        Union union = new Union(leftOp, rightOp, OpType.JOIN);
        root = union;
    }

    private void createIntersectionOp(Operator leftOp, Operator rightOp) {
        SortedRun leftRun = new SortedRun(leftOp, BufferManager.numBuffer);
        leftRun.setSchema(leftOp.getSchema());
        leftOp = new Distinct(leftRun, OpType.DISTINCT);
        leftOp.setSchema(leftRun.getSchema());
        SortedRun rightRun = new SortedRun(rightOp, BufferManager.numBuffer);
        rightRun.setSchema(rightOp.getSchema());
        rightOp = new Distinct(rightRun, OpType.DISTINCT);
        rightOp.setSchema(rightRun.getSchema());
        Intersect intersect = new Intersect(leftOp, rightOp, OpType.JOIN);
        root = intersect;
    }

}
