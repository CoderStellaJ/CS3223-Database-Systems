package qp.optimizer;

import qp.operators.Operator;
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
                    System.out.println("Invalid Set Operation Type");
            }
        }

        return this.root;
    }

    private void createUnionOp(Operator leftOp, Operator rightOp) {

    }

    private void createIntersectionOp(Operator leftOp, Operator rightOp) {

    }

}
