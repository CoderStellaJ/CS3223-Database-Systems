/**
 * This is base class for all the operators
 **/
package qp.operators;

import qp.utils.Batch;
import qp.utils.Schema;

public class Operator {

    int optype;     // Whether it is OpType.SELECT/ Optype.PROJECT/OpType.JOIN
    Schema schema;  // Schema of the result at this operator
    protected int batchsize;                  // Number of tuples per out batch

    public Operator(int type) {
        this.optype = type;
    }

    public Schema getSchema() {
        return schema;
    }

    public void setSchema(Schema schm) {
        this.schema = schm;
    }

    public int getOpType() {
        return optype;
    }

    public void setOpType(int type) {
        this.optype = type;
    }

    public boolean open() {
        System.err.println("Abstract interface cannot be used.");
        System.exit(1);
        return true;
    }

    public Batch next() {
        System.err.println("Abstract interface cannot be used.");
        System.exit(1);
        return null;
    }

    public boolean close() {
        return true;
    }

    public Object clone() {
        return new Operator(optype);
    }

    public void setSize() {
        /** select number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        // number of tuples in a page
        batchsize = Batch.getPageSize() / tuplesize;
        if (batchsize <= 0) {
            System.out.println(
                    String.format("The buffer size %d is not enough for tuples of size %d",
                            Batch.getPageSize(), tuplesize));
            System.exit(1);
        }
    }

}










