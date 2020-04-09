package qp.operators;

import qp.utils.Batch;
import qp.utils.Schema;
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
    TupleComparator comparator;
    ArrayList<LinkedList<Batch>> runs;

    /**
     * constructor
     **/
    public SortedRun(Operator base, int numBuffer) {
        super(OpType.SORT);
        this.base = base;
        this.numBuffer = numBuffer;
        this.comparator = new TupleComparator();
    }

    public SortedRun(Operator base, int numBuffer, int index) {
        super(OpType.SORT);
        this.base = base;
        this.numBuffer = numBuffer;
        this.comparator = new TupleComparator(index, index);
    }

    public SortedRun(Operator base, int numBuffer, ArrayList<Integer> indexes) {
        super(OpType.SORT);
        this.base = base;
        this.numBuffer = numBuffer;
        this.comparator = new TupleComparator(indexes, indexes);
    }

    public SortedRun(Operator base, int numBuffer, TupleComparator comparator) {
        super(OpType.SORT);
        this.base = base;
        this.numBuffer = numBuffer;
        this.comparator = comparator;
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    public TupleComparator getComparator() {
        return comparator;
    }

    public void setComparator(TupleComparator comparator) {
        this.comparator = comparator;
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
        outbatch = runs.get(0).pollFirst();
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

    @Override
    public Operator clone() {
        Operator newBase = (Operator) base.clone();
        SortedRun newSR = new SortedRun(newBase, numBuffer);
        newSR.setSchema((Schema) newBase.getSchema().clone());
        return newSR;
    }

    public ArrayList createSortedRuns() {
        ArrayList<LinkedList<Batch>> runs = new ArrayList<>();

        PriorityQueue<Tuple> pq = new PriorityQueue<>(comparator);
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
            if (outbatch.size() > 0) {
                run.add(outbatch);
            }
            if (run.size() >0) {
                runs.add(run);
            }
        }

        return runs;
    }

    public ArrayList mergeSortedRuns(ArrayList<LinkedList<Batch>> runs) {
        ArrayList<LinkedList<Batch>> newRuns = new ArrayList<>();
        int left = 0;
        int right = Math.min(numBuffer - 2, runs.size() - 1);
        while (left < runs.size()) {
            LinkedList<Batch> run = null;
            if (left == right) {
                run = runs.get(left);
            } else {
                run = mergeToOneRun(runs, left, right);
            }
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
        int smallestIndex = -1;
        Tuple smallestTuple = null;
        checkIndices(runs, left, right, indexes);

        while (!allPollOut(indexes)) {

            // Find the smallest tuple from the runs
            for (int i = left; i <= right; i++) {
                int index = indexes[i - left];
                if (index < 0) {
                    continue;
                }
                Tuple currentTuple = runs.get(i).peekFirst().get(index);
                if (smallestTuple == null || comparator.compare(smallestTuple, currentTuple) > 0) {
                    smallestIndex = i;
                    smallestTuple = currentTuple;
                }
            }
            //debugging printout
            /*
            if (smallestTuple != null) {
                System.out.println("==============");
                for (int i = 0; i < runs.get(smallestIndex).peekFirst().size(); i++) {
                    System.out.println(runs.get(smallestIndex).peekFirst().get(i).data());

                }
                System.out.println("----------");
                System.out.println(smallestTuple.data());
                System.out.println("==============");
            } */

            indexes[smallestIndex-left]++;
            if (runs.get(smallestIndex).peekFirst() != null && indexes[smallestIndex-left] >= runs.get(smallestIndex).peekFirst().size()) {
                runs.get(smallestIndex).pollFirst();
                indexes[smallestIndex-left] = 0;
            }

            outbatch.add(smallestTuple);
            smallestIndex = -1;
            smallestTuple = null;

            if (outbatch.isFull()) {
                run.add(outbatch);
                outbatch = new Batch(batchsize);
            }

            // Set the index of empty run to -1
            checkIndices(runs, left, right, indexes);
        }
        run.add(outbatch);
        return run;
    }

    public boolean allPollOut(int[] indices) {
        for (int i = 0; i < indices.length; i++) {
            if (indices[i] != -1) {
                return false;
            }
        }
        return true;
    }

    public boolean allPollOut(List<LinkedList<Batch>> runs, int left, int right) {
        for (int i = left; i <= right; i++) {
            if (!runs.get(i).isEmpty()) {
                return false;
            }
        }
        return true;
    }

    public int[] checkIndices(List<LinkedList<Batch>> runs, int left, int right, int[] indices) {
        for (int i = left; i <= right; i++) {
            if (runs.get(i).isEmpty() || runs.get(i).peekFirst() == null || runs.get(i).peekFirst().size() == 0) {
                indices[i-left] = -1;
            }
        }
        return indices;
    }
}
