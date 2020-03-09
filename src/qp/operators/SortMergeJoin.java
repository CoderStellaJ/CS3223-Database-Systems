package qp.operators;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Tuple;
import qp.operators.SortedRun;

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

    private int lcurs;                      // Cursor for left side sorted run
    private int rcurs;                      // Cursor for right side sorted run

    private boolean eof;                    // Whether end of stream (either left or right table) is reached

    public SortMergeJoin(Join jn) {
        super(new SortedRun(jn.getLeft(), jn.getNumBuff()), new SortedRun(jn.getRight(), jn.getNumBuff()),
                jn.getConditionList(), jn.getOpType());
        schema = jn.getSchema();
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
        initializeCursors();
        return materializeTable();
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
        eof = false;
    }

    public boolean materializeTable() {

        /** Right hand side table is to be materialized
         ** for the Nested join to perform
         **/
        if (!left.open() || !right.open()) {
            return false;
        } else {
            /** If the right operator is not a base table then
             ** Materialize the intermediate result from right
             ** into a file
             **/
            filenum++;
            lfname = "lBNJtemp-" + String.valueOf(filenum);
            rfname = "rBNJtemp-" + String.valueOf(filenum);
            try {
                writeToFile(lfname, left);
                writeToFile(rfname, right);
            } catch (IOException io) {
                System.out.println("SortMergeJoin: Error writing to temporary file");
                return false;
            }
            if (!left.close() || !right.close())
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
        if (eof) {
            return null;
        }
        outbatch = new Batch(batchsize);
        // enter while loop when output buffer is not full
        while (!outbatch.isFull()) {
            // right buffer reaches end, load another block of left buffers
            if (eof) {
                return outbatch;
            }

            // load in pages of right table one by one
            while (eof == false) {
                try {
                    leftbatch = (Batch) leftin.readObject();
                    rightbatch = (Batch) rightin.readObject();
                    // Todo: perform join on two sorted files
                } catch (EOFException e) {
                    try {
                        leftin.close();
                        rightin.close();
                    } catch (IOException io) {
                        System.out.println("SortMergeJoin: Error in reading temporary file");
                    }
                    eof = true;
                } catch (ClassNotFoundException c) {
                    System.out.println("SortMergeJoin: Error in deserialising temporary file ");
                    System.exit(1);
                } catch (IOException io) {
                    System.out.println("SortMergeJoin: Error in reading temporary file");
                    System.exit(1);
                }
            }
        }
        return outbatch;
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
