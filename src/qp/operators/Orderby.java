package qp.operators;

import qp.utils.*;

import java.util.ArrayList;
import java.util.Comparator;

public class Orderby extends Operator {

    Operator base;                 // Base table to sort
    ArrayList<Attribute> attrset;  // Set of attributes to sort
    int numBuff;

    ArrayList<Integer> attrIndex;

    public Orderby(Operator base, ArrayList<Attribute> as, int numBuff) {
        super(OpType.ORDERBY);
        this.base = base;
        this.attrset = as;
        this.numBuff = numBuff;
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    public boolean open() {
        setSize();

        Schema baseSchema = base.getSchema();
        attrIndex = new ArrayList<>();
        for (int i = 0; i < attrset.size(); ++i) {
            Attribute attr = attrset.get(i);

            if (attr.getAggType() != Attribute.NONE) {
                System.err.println("Aggragation is not implemented.");
                System.exit(1);
            }

            int index = baseSchema.indexOf(attr.getBaseAttribute());
            attrIndex.add(index);
        }
        this.base = new SortedRun(this.base, numBuff);
        ((SortedRun) this.base).setComparator(new TupleComparator(attrIndex, attrIndex));
        this.base.setSchema(schema);

        if (!base.open()) return false;

        return true;
    }

    public Batch next() {
        return base.next();
    }

    public boolean close() {
        base.close();
        return true;
    }

    public Object clone() {
        Operator newbase = (Operator) base.clone();
        ArrayList<Attribute> newattr = new ArrayList<>();
        for (int i = 0; i < attrset.size(); ++i)
            newattr.add((Attribute) attrset.get(i).clone());
        Orderby newob = new Orderby(newbase, newattr, numBuff);
        Schema newSchema = (Schema) newbase.getSchema().subSchema(newattr).clone();
        newob.setSchema(newSchema);
        return newob;
    }

}

