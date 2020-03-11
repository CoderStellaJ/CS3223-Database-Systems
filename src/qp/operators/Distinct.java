package qp.operators;

import qp.utils.Batch;
import qp.utils.Tuple;

public class Distinct extends Operator {

    Operator base;
    public Tuple prev = null;

    boolean eos;     // Indicate whether end of stream is reached or not
    Batch inbatch;   // This is the current input buffer
    Batch outbatch;  // This is the current output buffer
    int start;       // Cursor position in the input buffer
    /**
     * constructor
     *
     * @param base
     * @param type
     */
    public Distinct(Operator base, int type) {
        super(type);
        this.base = base;
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    @Override
    public boolean open() {
        eos = false;  // Since the stream is just opened
        start = 0;    // Set the cursor to starting position in input buffer

        setSize();

        if (base.open())
            return true;
        else
            return false;
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
                if (!isDuplicate(present)) {
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

    public boolean close() {
        base.close();    // Added base.close
        return true;
    }

    public boolean isDuplicate(Tuple curr) {
        if (this.prev == null || !this.prev.equals(curr)) {
            this.prev = curr;
            return false;
        }
        return true;
    }
}
