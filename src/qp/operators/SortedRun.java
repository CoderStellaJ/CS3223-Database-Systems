package qp.operators;

import qp.utils.Batch;
import qp.utils.Tuple;
import qp.utils.TupleComparator;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;

public class SortedRun extends Operator {
    Operator base;  // Base operator
    int batchsize;  // Number of tuples per outbatch
    int numBuffer;   // This indicates the number of buffers available

    boolean eos;     // Indicate whether end of stream is reached or not
    Batch inbatch;   // This is the current input buffer
    Batch outbatch;  // This is the current output buffer
    int start;       // Cursor position in the input buffer
    List<LinkedList<Batch>> runs;

    /**
     * constructor
     **/
    public SortedRun(Operator base, int type, int numBuffer) {
        super(type);
        this.base = base;
        this.numBuffer = numBuffer;
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    /**
     * Opens the connection to the base operator
     **/
    public boolean open() {
        eos = false;  // Since the stream is just opened
        start = 0;    // Set the cursor to starting position in input buffer

        /** Set number of tuples per page**/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        if (!base.open()) {
            return false;
        }

        runs = createSortedRuns();

        // TODO: Should stop when run.size() > numBuffer - 1, but it will be too complicated for next(), so do it later
        while (runs.size() > 1) {
            runs = mergeSortedRuns(runs);
        }
        return true;
    }

    /**
     * returns a batch of tuples that satisfies the
     * * condition specified on the tuples coming from base operator
     * * NOTE: This operation is performed on the fly
     **/
    public Batch next() {
        if (allPollOut(runs, 0, 0)) {
            return null;
        }
        outbatch = runs.get(0).poll();
        return outbatch;
    }

    /**
     * closes the output connection
     * * i.e., no more pages to output
     **/
    public boolean close() {
        base.close();    // Added base.close
        return true;
    }

    public List createSortedRuns() {
        List<LinkedList<Batch>> runs = new ArrayList<>();

        // TODO: currently only compare tuple by their first attribue
        PriorityQueue<Tuple> pq = new PriorityQueue<>(new TupleComparator());
        while (!eos) {
            for (int i = 0; i < numBuffer; i++) {
                inbatch = base.next();
                if (inbatch == null) {
                    eos = true;
                    break;
                }
                for (int j = 0; j < inbatch.size(); j++) {
                    pq.add(inbatch.get(j));
                }
            }
            LinkedList<Batch> run = new LinkedList<>();
            outbatch = new Batch(batchsize);
            while (!pq.isEmpty()) {
                outbatch.add(pq.poll());
                if (outbatch.isFull()) {
                    run.add(outbatch);
                    outbatch = new Batch(batchsize);
                }
            }
            runs.add(run);
        }

        return runs;
    }

    public List mergeSortedRuns(List<LinkedList<Batch>> runs) {
        List<LinkedList<Batch>> newRuns = new ArrayList<>();
        int left = 0;
        int right = Math.min(numBuffer - 1, runs.size() - 1);
        while (left < runs.size()) {
            LinkedList<Batch> run = mergeToOneRun(runs, left, right);
            newRuns.add(run);
            left = right + 1;
            right = Math.min(left + numBuffer - 2, runs.size() - 1);
        }

        return newRuns;
    }

    public LinkedList<Batch> mergeToOneRun(List<LinkedList<Batch>> runs, int left, int right) {
        LinkedList<Batch> run = new LinkedList<>();

        // Keep track the curr index of tuple in each batch
        int[] indexes = new int[right - left + 1];
        int smallestIndex = left;
        Tuple smallestTuple = runs.get(left).get(0).get(0);

        while (!allPollOut(runs, left, right)) {
            outbatch = new Batch(batchsize);
            while (!outbatch.isFull()) {
                for (int i = left; i <= right; i++) {
                    int index = indexes[i - left];
                    if (Tuple.compareTuples(smallestTuple, runs.get(i).peek().get(index), 0) > 0) {
                        smallestIndex = i;
                        smallestTuple = runs.get(i).peek().get(index);
                    }
                }
                outbatch.add(runs.get(smallestIndex).peek().get(indexes[smallestIndex-left]));
                indexes[smallestIndex-left]++;
                if (indexes[smallestIndex-left] >= runs.get(smallestIndex).peek().size()) {
                    runs.get(smallestIndex).poll();
                    indexes[smallestIndex-left] = 0;
                }
            }
            run.add(outbatch);
        }

        return run;
    }

    public boolean allPollOut(List<LinkedList<Batch>> runs, int left, int right) {
        for (int i = left; i <= right; i++) {
            if (!runs.get(i).isEmpty()) {
                return false;
            }
        }
        return true;
    }
}
