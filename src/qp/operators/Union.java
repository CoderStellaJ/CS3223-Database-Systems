package qp.operators;

import qp.utils.*;

import java.util.ArrayList;
import java.util.List;

public class Union extends Join {

    ArrayList<Integer> leftindex;   // Indices of the join attributes in left table
    ArrayList<Integer> rightindex;  // Indices of the join attributes in right table
    int lcurs;                      // Cursor for left side buffer
    int rcurs;                      // Cursor for right side buffer
    Batch outbatch;                 // Buffer page for output
    Batch leftbatch;                // Buffer page for left input stream
    Batch rightbatch;               // Buffer page for right input stream
    boolean eosl;                   // Whether end of stream (left table) is reached
    boolean eosr;                   // Whether end of stream (right table) is reached
    ArrayList<Integer> tupleAttributeIndexes;

    public Union(Operator left, Operator right, int type) {
        super(left, right, type);
        Schema leftSchema = left.getSchema();
        Schema rightSchema = right.getSchema();
        List<Attribute> leftAttrList = leftSchema.getAttList();
        List<Attribute> rightAttrList = rightSchema.getAttList();
        if (leftAttrList.size() != rightAttrList.size()) {
            System.err.println("Invalid Union");
            System.exit(1);
        }
        ArrayList<Condition> conditionList = new ArrayList<>();
        for (int i = 0; i < leftAttrList.size(); i++) {
            if (!leftAttrList.get(i).equals(rightAttrList.get(i))) {
                System.err.println("Invalid Union");
                System.exit(1);
            }
            Condition con = new Condition(leftAttrList.get(i), Condition.EQUAL, rightAttrList.get(i));
            conditionList.add(con);
        }
        this.setConditionList(conditionList);
        schema = leftSchema;
        jointype = JoinType.UNION;
        this.tupleAttributeIndexes = new ArrayList<>();
        for (int i = 0; i < leftAttrList.size(); i++) {
            this.tupleAttributeIndexes.add(i);
        }
    }

    @Override
    public boolean open() {
        setSize();
        leftindex = new ArrayList<>();
        rightindex = new ArrayList<>();
        for (Condition con : conditionList) {
            Attribute leftattr = con.getLhs();
            Attribute rightattr = (Attribute) con.getRhs();
            leftindex.add(left.getSchema().indexOf(leftattr));
            rightindex.add(right.getSchema().indexOf(rightattr));
        }

        /** initialize the cursors of input buffers **/
        lcurs = 0;
        rcurs = 0;
        if (left.open() && right.open()) {
            return true;
        }
        return false;
    }

    @Override
    public Batch next() {
        if (eosl || eosr) {
            close();
            return null;
        }

        outbatch = new Batch(batchsize);
        while (!outbatch.isFull()) {
            if (lcurs == 0) {
                leftbatch = left.next();
                /** There is no more incoming pages from left operator **/
                if (leftbatch == null) {
                    eosl = true;
                    return outbatch;
                }
            }
            if (rcurs == 0) {
                rightbatch = right.next();
                /** There is no more incoming pages from right operator **/
                if (rightbatch == null) {
                    eosr = true;
                    return outbatch;
                }
            }

            while (!outbatch.isFull() && lcurs < leftbatch.size() && rcurs < rightbatch.size()) {
                Tuple leftTuple = leftbatch.get(lcurs);
                Tuple rightTuple = rightbatch.get(rcurs);
                // always add the smaller tuple into the outbatch
                if (leftTuple.equals(rightTuple)) {
                    outbatch.add(leftTuple);
                    lcurs++;
                    rcurs++;
                } else if (Tuple.compareTuples(leftTuple, rightTuple, tupleAttributeIndexes, tupleAttributeIndexes) < 0) {
                    outbatch.add(leftTuple);
                    lcurs++;
                } else {
                    outbatch.add(rightTuple);
                    rcurs++;
                }
            }
            while(!outbatch.isFull() && lcurs < leftbatch.size() && rcurs == rightbatch.size()) {
                Tuple leftTuple = leftbatch.get(lcurs);
                outbatch.add(leftTuple);
                lcurs++;
            }
            while(!outbatch.isFull() && rcurs < rightbatch.size() && lcurs == leftbatch.size()) {
                Tuple rightTuple = rightbatch.get(rcurs);
                outbatch.add(rightTuple);
                rcurs++;
            }

            if (lcurs == leftbatch.size()) {
                lcurs = 0;
            }
            if (rcurs == rightbatch.size()) {
                rcurs = 0;
            }
        }
        return outbatch;
    }

    @Override
    public boolean close() {
        left.close();
        right.close();
        return true;
    }

    @Override
    public Operator clone() {
        Operator newLeft = (Operator) left.clone();
        Operator newRight = (Operator) right.clone();
        Union newUnion = new Union(newLeft, newRight, this.getOpType());
        newUnion.setSchema((Schema) newLeft.getSchema().clone());
        return newUnion;
    }

}
