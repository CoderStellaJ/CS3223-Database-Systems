package qp.operators;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Tuple;

public class BlockNestedJoin extends Join{

    private static int filenum = 0;         // To get unique filenum for this operation

    private int batchsize;                  // Number of tuples per out batch
    private ArrayList<Integer> leftindex;   // Indices of the join attributes in left table
    private ArrayList<Integer> rightindex;  // Indices of the join attributes in right table
    private String rfname;                  // The file name where the right table is materialized
    private Batch outbatch;                 // Buffer page for output
    private List<Batch> leftblock;        // a block of buffer pages for left input stream
    private Batch rightbatch;               // Buffer page for right input stream
    private ObjectInputStream in;           // File pointer to the right hand materialized file

    private int lcurs;                      // Cursor for left side buffer
    private int rcurs;                      // Cursor for right side buffer
    private int lpage;                      // Cursor for left side block

    private boolean eosl;                   // Whether end of stream (left table) is reached
    private boolean eosr;                   // Whether end of stream (right table) is reached

    public BlockNestedJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getConditionList(), jn.getOpType());
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
        leftblock = new ArrayList<Batch>();
        return materializeTable();
    }

    public void setSize() {
        /** select number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        // number of tuples in a page
        batchsize = Batch.getPageSize() / tuplesize;
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
        eosl = false;
        /** because right stream is to be repetitively scanned
         ** if it reached end, we have to start new scan
         **/
        eosr = true;
    }

    public boolean materializeTable() {
        Batch rightpage;
        /** Right hand side table is to be materialized
         ** for the Nested join to perform
         **/
        if (!right.open()) {
            return false;
        } else {
            /** If the right operator is not a base table then
             ** Materialize the intermediate result from right
             ** into a file
             **/
            filenum++;
            rfname = "BNJtemp-" + String.valueOf(filenum);
            try {
                ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rfname));
                while ((rightpage = right.next()) != null) {
                    out.writeObject(rightpage);
                }
                out.close();
            } catch (IOException io) {
                System.out.println("BlockNestedJoin: Error writing to temporary file");
                return false;
            }
            if (!right.close())
                return false;
        }
        if (left.open())
            return true;
        else
            return false;
    }

    /**
     * from input buffers selects the tuples satisfying join condition
     * * And returns a page of output tuples
     **/
    public Batch next() {
        int i, j, k;
        // reaches the end of the left file
        if (eosl) {
            return null;
        }
        outbatch = new Batch(batchsize);
        // enter while loop when output buffer is not full
        while (!outbatch.isFull()) {
            // right buffer reaches end, load another block of left buffers
            if (lcurs == 0 && eosr == true) {
                /** new left page is to be fetched**/
                loadLeftBlock();
                if(leftblock.isEmpty()) {
                    eosl = true;
                    return outbatch;
                }
                StartScanRightTable();
            }

            // load in pages of right table one by one
            while (eosr == false) {
                try {
                    if (rcurs == 0 && lcurs == 0) {
                        rightbatch = (Batch) in.readObject();
                    }

                    for(i = lpage; i < leftblock.size(); ++i) {
                        Batch leftbatch = leftblock.get(i);
                        for (j = lcurs; j < leftbatch.size(); ++j) {
                            Tuple lefttuple = leftbatch.get(j);
                            for (k = rcurs; k < rightbatch.size(); ++k) {
                                Tuple righttuple = rightbatch.get(k);
                                if (lefttuple.checkJoin(righttuple, leftindex, rightindex)) {
                                    Tuple outtuple = lefttuple.joinWith(righttuple);
                                    outbatch.add(outtuple);
                                    if (outbatch.isFull()) {
                                        if(k != rightbatch.size()-1) {
                                            // case 1: rightbatch is not finished
                                            lpage = i;
                                            lcurs = j;
                                            rcurs = k+1;
                                        } else if (j != leftbatch.size()-1) {
                                            // case 2: rightbatch is finished, but leftbatch is not finished
                                            lpage = i;
                                            lcurs = j+1;
                                            rcurs = 0;
                                        } else if (i != leftblock.size()-1) {
                                            // case 3: rightbatch is finished, leftbatch is also finished
                                            // leftblock move to the next batch
                                            lpage = i+1;
                                            lcurs = 0;
                                            rcurs = 0;
                                        } else {
                                            // case 4: leftblock are finished
                                            lpage = 0;
                                            lcurs = 0;
                                            rcurs = 0;
                                        }
                                        return outbatch;
                                    }
                                }
                            }
                            rcurs = 0;
                        }
                        lcurs = 0;
                    }
                    lpage = 0;
                } catch (EOFException e) {
                    try {
                        in.close();
                    } catch (IOException io) {
                        System.out.println("BlockNestedJoin: Error in reading temporary file");
                    }
                    eosr = true;
                } catch (ClassNotFoundException c) {
                    System.out.println("BlockNestedJoin: Error in deserialising temporary file ");
                    System.exit(1);
                } catch (IOException io) {
                    System.out.println("BlockNestedJoin: Error in reading temporary file");
                    System.exit(1);
                }
            }
        }
        return outbatch;
    }


    public void loadLeftBlock() {
        // load in a new block of pages
        leftblock.clear();
        // load in B-2 pages
        for(int i = 0; i < this.numBuff-2; i++) {
            Batch newBatch = (Batch) left.next();
            if (newBatch != null) {
                leftblock.add(newBatch);
            }
        }
    }


    public void StartScanRightTable() {
        /** Whenever a new left page came, we have to start the
         ** scanning of right table
         **/
        try {
            in = new ObjectInputStream(new FileInputStream(rfname));
            eosr = false;
        } catch (IOException io) {
            System.err.println("NestedJoin:error in reading the file");
            System.exit(1);
        }
    }

    /**
     * Close the operator
     */
    public boolean close() {
        File f = new File(rfname);
        f.delete();
        return true;
    }

}
