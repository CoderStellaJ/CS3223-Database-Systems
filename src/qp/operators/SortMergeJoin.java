package qp.operators;

import java.io.*;
import java.util.ArrayList;

import qp.utils.*;

public class SortMergeJoin extends Join{

    private static int filenum = 2;         // To get unique filenum for this operation

    private ArrayList<Integer> leftindex;   // Indices of the join attributes in left table
    private ArrayList<Integer> rightindex;  // Indices of the join attributes in right table
    private String lfname;                  // The file name where the left table is materialized
    private String rfname;                  // The file name where the right table is materialized
    private Batch outbatch;                 // Buffer page for output
    private Batch leftbatch;                // Buffer page for left input stream
    private Batch rightbatch;               // Buffer page for right input stream
    private ObjectInputStream leftin;       // File pointer to the left hand materialized file
    private ObjectInputStream rightin;      // File pointer to the right hand materialized file
    private ArrayList<Tuple> backtrackbuffer;// Buffer to store the tuples for backtracking
    private int backtrackcurs;              // Cursor for backtracking buffer
    private Tuple prevlefttuple;            // The previous accessed left tuple for backtracking
    private boolean ifBacktracking;         // Indicate whether we are joining with tupples in backtrackbuffer

    private int lcurs;                      // Cursor for left side sorted run
    private int rcurs;                      // Cursor for right side sorted run
    boolean eosl;                           // Whether end of stream (left table) is reached
    boolean eosr;                           // Whether end of stream (right table) is reached

    TupleComparator comparator;             // To compare between left and right tuples by their join index
    TupleComparator leftcomparator;         // To compare between left tuples by its join index

    public SortMergeJoin(Join jn) {
        super(new SortedRun(jn.getLeft(), jn.getNumBuff()), new SortedRun(jn.getRight(), jn.getNumBuff()),
                jn.getConditionList(), jn.getOpType());
        schema = jn.getSchema();
        left.setSchema(jn.getLeft().getSchema());
        right.setSchema(jn.getRight().getSchema());
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }

    /**
     * During open finds the index of the join attributes
     * * Materializes the right hand side into a file
     * * Opens the connections
     **/
    public boolean open() {
        setSize();
        setAttributeIndex();
        setComparator();
        initializeCursors();
        return materializeTable();
    }

    private void setComparator() {
        comparator = new TupleComparator(leftindex, rightindex);
        leftcomparator = new TupleComparator(leftindex, leftindex);
    }

    public void setAttributeIndex() {
        /** find indices attributes of join conditions **/
        leftindex = new ArrayList<>();
        rightindex = new ArrayList<>();
        for (Condition con : conditionList) {
            Attribute leftattr = con.getLhs();
            Attribute rightattr = (Attribute) con.getRhs();
            leftindex.add(left.getSchema().indexOf(leftattr));
            rightindex.add(right.getSchema().indexOf(rightattr));
        }
    }

    public void initializeCursors() {
        /** initialize the cursors of input buffers **/
        lcurs = 0;
        rcurs = 0;

        backtrackcurs = 0;
        backtrackbuffer = new ArrayList<>();

        eosl = false;
        eosr = false;
    }

    public boolean materializeTable() {

        // Set comparator so that the table is sorted by specified indices
        SortedRun leftSorted = (SortedRun) this.left;
        SortedRun rightSorted = (SortedRun) this.right;
        leftSorted.setComparator(new TupleComparator(leftindex, leftindex));
        rightSorted.setComparator(new TupleComparator(rightindex, rightindex));
        leftSorted.numBuffer = numBuff;
        rightSorted.numBuffer = numBuff;

        if (!leftSorted.open() || !rightSorted.open()) {
            return false;
        } else {

            filenum++;
            lfname = "lBNJtemp-" + String.valueOf(filenum);
            rfname = "rBNJtemp-" + String.valueOf(filenum);
            try {
                writeToFile(lfname, leftSorted);
                writeToFile(rfname, rightSorted);
            } catch (IOException io) {
                System.out.println("SortMergeJoin: Error writing to temporary file");
                return false;
            }
            if (!leftSorted.close() || !rightSorted.close())
                return false;
            else
                StartScanTable();
                return true;
        }
    }

    private void writeToFile(String fileName, Operator operator) throws IOException {
        ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(fileName));
        Batch page;
        while ((page = operator.next()) != null) {
            out.writeObject(page);
        }
        out.close();
    }

    /**
     * from input buffers selects the tuples satisfying join condition
     * * And returns a page of output tuples
     **/
    public Batch next() {
        // reaches the end of the left file
        if (eosl) {
            return null;
        }
        outbatch = new Batch(batchsize);

        // enter while loop when output buffer is not full
        while (!outbatch.isFull()) {
            try {
                // load in new pages from tables
                if (!eosl && (leftbatch == null || lcurs >= leftbatch.size())) {
                    try {
                        leftbatch = (Batch) leftin.readObject();
                    } catch (EOFException e) {
                        eosl = true;
                        leftin.close();
                    }
                    lcurs = 0;
                }
                if (!eosr && (rightbatch == null || rcurs >= rightbatch.size())) {
                    try {
                        rightbatch = (Batch) rightin.readObject();
                    } catch (EOFException e) {
                        rightin.close();
                        eosr = true;
                    }
                    rcurs = 0;
                }

                if (eosl) {
                    return outbatch;
                } else if (ifBacktracking) {
                    joinBacktrackBuffer();
                } else if (!eosl && eosr) {
                    // The left table hasn't reached to the end, the ifBacktracking flag is not true
                    // but still need to backtracking just in case
                    joinBacktrackBuffer();

                    // If no more backtracking is needed, its the same as we are done
                    if (!ifBacktracking) {
                        eosl = true;
                    }
                } else {
                    while (lcurs < leftbatch.size() && rcurs < rightbatch.size()) {
                        Tuple lefttuple = leftbatch.get(lcurs);
                        Tuple righttuple = rightbatch.get(rcurs);
                        if (lefttuple.checkJoin(righttuple, leftindex, rightindex)) {
                            backtrackbuffer.add(righttuple);
                            Tuple outtuple = lefttuple.joinWith(righttuple);
                            outbatch.add(outtuple);
                            rcurs += 1;

                            if (outbatch.isFull()) {
                                return outbatch;
                            }
                        } else if (comparator.compare(lefttuple, righttuple) < 0) {
                            // lefttuple < righttuple
                            lcurs += 1;
                            prevlefttuple = lefttuple;

                            // Check for backtracking
                            joinBacktrackBuffer();
                            break;

                        } else {
                            // lefttuple > righttuple
                            rcurs += 1;
                        }
                    }
                }
            } catch (ClassNotFoundException c) {
                System.out.println("SortMergeJoin: Error in deserialising temporary file ");
                System.exit(1);
            } catch (IOException io) {
                System.out.println("SortMergeJoin: Error in reading temporary file");
                System.exit(1);
            }
        }
        return outbatch;
    }

    private void joinBacktrackBuffer() {

        // Exit the function when
        // 1. the outbatch is full
        // 2. lefttuple is different from the previous one, no more backtracking is needed
        // 3. leftbatch reaches to the end, need to fetch new batch from left table

        ifBacktracking = true;

        while (lcurs < leftbatch.size()) {
            Tuple lefttuple = leftbatch.get(lcurs);
            if (prevlefttuple != null && leftcomparator.compare(lefttuple, prevlefttuple) != 0) {

                // Clear buffer and stop backtracking
                backtrackbuffer = new ArrayList<>();
                ifBacktracking = false;
                return;

            } else {

                if (backtrackcurs == backtrackbuffer.size()) {
                    // Restart cursor
                    backtrackcurs = 0;
                }

                // Join with tuples in buffer one by one
                while (backtrackcurs < backtrackbuffer.size()) {
                    Tuple outtuple = lefttuple.joinWith(backtrackbuffer.get(backtrackcurs));
                    outbatch.add(outtuple);
                    backtrackcurs += 1;

                    if (outbatch.isFull()) {
                        if (backtrackcurs == backtrackbuffer.size()) {
                            prevlefttuple = lefttuple;
                            lcurs += 1;
                        }
                        // Return to the main function
                        return;
                    }
                }
                // check for next left tuple
                prevlefttuple = lefttuple;
                lcurs += 1;
            }

        }

    }


    public void StartScanTable() {
        try {
            leftin = new ObjectInputStream(new FileInputStream(lfname));
            rightin = new ObjectInputStream(new FileInputStream(rfname));
        } catch (IOException io) {
            System.err.println("SortMergeJoin: error in reading the file");
            System.exit(1);
        }
    }

    /**
     * Close the operator
     */
    public boolean close() {
        File lf = new File(lfname);
        lf.delete();
        File rf = new File(rfname);
        rf.delete();
        return true;
    }

}
