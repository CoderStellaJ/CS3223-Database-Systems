package qp.operators;

import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Tuple;

public class Distinct extends Select {

    public Tuple prev = null;
    public boolean needCondition;
    /**
     * constructor
     *
     * @param base
     * @param con
     * @param type
     */
    public Distinct(Operator base, Condition con, int type, boolean needCondition) {
        super(base, con, type);
        this.needCondition = needCondition;
    }

    @Override
    public Batch next() {
        int i = 0;
        if (eos) {
            close();
            return null;
        }

        outbatch = new Batch(batchsize);

        while (!outbatch.isFull()) {
            if (start == 0) {
                inbatch = base.next();
                /** There is no more incoming pages from base operator **/
                if (inbatch == null) {
                    eos = true;
                    return outbatch;
                }
            }

            /** Continue this for loop until this page is fully observed
             ** or the output buffer is full
             **/
            for (i = start; i < inbatch.size() && (!outbatch.isFull()); ++i) {
                Tuple present = inbatch.get(i);

                /**
                 *  Only checkCondition if it is required so as conditions might be checked during previous pass
                 */
                if (!isDuplicate(present) && (!this.needCondition || checkCondition(present))) {
                    outbatch.add(present);
                }
            }

            /** Modify the cursor to the position requierd
             ** when the base operator is called next time;
             **/
            if (i == inbatch.size())
                start = 0;
            else
                start = i;
        }
        return outbatch;
    }

    public boolean isDuplicate(Tuple curr) {
        if (this.prev == null || !this.prev.equals(curr)) {
            this.prev = curr;
            return false;
        }
        return true;
    }
}
