/**
 * performs randomized optimization, iterative improvement algorithm
 **/

package qp.optimizer;

import qp.operators.*;
import qp.utils.Attribute;
import qp.utils.Condition;
import qp.utils.RandNumb;
import qp.utils.SQLQuery;

import java.util.ArrayList;
import java.util.List;

public class RandomOptimizer {

    /**
     * enumeration of different ways to find the neighbor plan
     **/
    public static final int METHODCHOICE = 0;  // Selecting neighbor by changing a method for an operator
    public static final int COMMUTATIVE = 1;   // By rearranging the operators by commutative rule
    public static final int ASSOCIATIVE = 2;   // Rearranging the operators by associative rule
    public static final int SORT_POSITION_CHOICE = 3; // Rearranging position of sort

    /**
     * Number of altenative methods available for a node as specified above
     **/
    public static final int NUMCHOICES = 4;

    SQLQuery sqlquery;  // Vector of Vectors of Select + From + Where + GroupBy
    int numJoin;        // Number of joins in this query plan

    /**
     * constructor
     **/

    public RandomOptimizer(SQLQuery sqlquery) {
        this.sqlquery = sqlquery;
    }

    /**
     * After finding a choice of method for each operator
     * * prepare an execution plan by replacing the methods with
     * * corresponding join operator implementation
     **/
    public static Operator makeExecPlan(Operator node) {
        if (node.getOpType() == OpType.JOIN) {
            Operator left = makeExecPlan(((Join) node).getLeft());
            Operator right = makeExecPlan(((Join) node).getRight());
            int joinType = ((Join) node).getJoinType();
            int numbuff = BufferManager.getBuffersPerJoin();
            switch (joinType) {
                case JoinType.NESTEDJOIN:
                    NestedJoin nj = new NestedJoin((Join) node);
                    nj.setLeft(left);
                    nj.setRight(right);
                    nj.setNumBuff(numbuff);
                    return nj;
                case JoinType.BLOCKNESTED:
                    BlockNestedJoin bnj = new BlockNestedJoin((Join) node);
                    bnj.setLeft(left);
                    bnj.setRight(right);
                    bnj.setNumBuff(numbuff);
                    return bnj;
                case JoinType.SORTMERGE:
                    SortMergeJoin smj = new SortMergeJoin((Join) node);
                    smj.setNumBuff(numbuff);
                    return smj;
                default:
                    return node;
            }
        } else if (node.getOpType() == OpType.SELECT) {
            Operator base = makeExecPlan(((Select) node).getBase());
            ((Select) node).setBase(base);
            return node;
        } else if (node.getOpType() == OpType.PROJECT) {
            Operator base = makeExecPlan(((Project) node).getBase());
            ((Project) node).setBase(base);
            return node;
        } else if (node.getOpType() == OpType.DISTINCT) {
            Operator base = makeExecPlan(((Distinct) node).getBase());
            ((Distinct) node).setBase(base);
            return node;
        } else if (node.getOpType() == OpType.SORT) {
            Operator base = makeExecPlan(((SortedRun) node).getBase());
            ((SortedRun) node).setBase(base);
            return node;
        } else if (node.getOpType() == OpType.ORDERBY) {
            Operator base = makeExecPlan(((Orderby) node).getBase());
            ((Orderby) node).setBase(base);
            return node;
        } else {
            return node;
        }
    }

    /**
     * Randomly selects a neighbour
     **/
    protected Operator getNeighbor(Operator root) {
        // Randomly select a node to be altered to get the neighbour
        int nodeNum = RandNumb.randInt(0, numJoin - 1);
        // Randomly select type of alteration: Change Method/Associative/Commutative
        int changeType = RandNumb.randInt(0, NUMCHOICES - 1);
        Operator neighbor = null;
        switch (changeType) {
            case METHODCHOICE:   // Select a neighbour by changing the method type
                neighbor = neighborMeth(root, nodeNum);
                break;
            case COMMUTATIVE:
                neighbor = neighborCommut(root, nodeNum);
                break;
            case ASSOCIATIVE:
                neighbor = neighborAssoc(root, nodeNum);
                break;
            case SORT_POSITION_CHOICE:
                neighbor = neighborSortPosition(root, nodeNum);
                break;
        }
        return neighbor;
    }

    /**
     * Implementation of Iterative Improvement Algorithm for Randomized optimization of UnprocessedQuery Plan
     **/
    public Operator getOptimizedPlan() {
        /** get an initial plan for the given sql query **/
        RandomInitialPlan rip = new RandomInitialPlan(sqlquery);
        numJoin = rip.getNumJoins();
        long MINCOST = Long.MAX_VALUE;
        Operator finalPlan = null;

        /** NUMITER is number of times random restart **/
        int NUMITER;
        if (numJoin != 0) {
            NUMITER = 2 * numJoin;
        } else {
            NUMITER = 1;
        }

        /** Randomly restart the gradient descent until
         *  the maximum specified number of random restarts (NUMITER)
         *  has satisfied
         **/
        for (int j = 0; j < NUMITER; ++j) {
            Operator initPlan = rip.prepareInitialPlan();
            modifySchema(initPlan);
            System.out.println("-----------initial Plan-------------");
            Debug.PPrint(initPlan);
            PlanCost pc = new PlanCost();
            long initCost = pc.getCost(initPlan);
            System.out.println(initCost);

            boolean flag = true;
            long minNeighborCost = initCost;   //just initialization purpose;
            Operator minNeighbor = initPlan;  //just initialization purpose;
            if (numJoin != 0) {
                while (flag) {  // flag = false when local minimum is reached
                    System.out.println("---------------while--------");
                    Operator initPlanCopy = (Operator) initPlan.clone();
                    minNeighbor = getNeighbor(initPlanCopy);

                    System.out.println("--------------------------neighbor---------------");
                    Debug.PPrint(minNeighbor);
                    pc = new PlanCost();
                    minNeighborCost = pc.getCost(minNeighbor);
                    System.out.println("  " + minNeighborCost);

                    /** In this loop we consider from the
                     ** possible neighbors (randomly selected)
                     ** and take the minimum among for next step
                     **/
                    for (int i = 1; i < 2 * numJoin; ++i) {
                        initPlanCopy = (Operator) initPlan.clone();
                        Operator neighbor = getNeighbor(initPlanCopy);
                        System.out.println("------------------neighbor--------------");
                        Debug.PPrint(neighbor);
                        pc = new PlanCost();
                        long neighborCost = 0;
                        try {
                            neighborCost = pc.getCost(neighbor);
                        } catch (Exception e) {
                            System.out.println("fatal error.");
                            System.exit(0);
                        }
                        System.out.println(neighborCost);

                        if (neighborCost < minNeighborCost) {
                            minNeighbor = neighbor;
                            minNeighborCost = neighborCost;
                        }
                    }
                    if (minNeighborCost < initCost) {
                        initPlan = minNeighbor;
                        initCost = minNeighborCost;
                    } else {
                        minNeighbor = initPlan;
                        minNeighborCost = initCost;
                        flag = false;  // local minimum reached
                    }
                }
                System.out.println("------------------local minimum--------------");
                Debug.PPrint(minNeighbor);
                System.out.println(" " + minNeighborCost);
            }
            if (minNeighborCost < MINCOST) {
                MINCOST = minNeighborCost;
                finalPlan = minNeighbor;
            }
        }
        System.out.println("\n\n\n");
        System.out.println("---------------------------Final Plan----------------");
        Debug.PPrint(finalPlan);
        System.out.println("  " + MINCOST);
        return finalPlan;
    }

    /**
     * Selects a random method choice for join wiht number joinNum
     * *  e.g., Nested loop join, Sort-Merge Join, Hash Join etc..,
     * * returns the modified plan
     **/

    protected Operator neighborMeth(Operator root, int joinNum) {
        System.out.println("------------------neighbor by method change----------------");
        int numJMeth = JoinType.numJoinTypes();
        if (numJMeth > 1) {
            /** find the node that is to be altered **/
            Join node = (Join) findNodeAt(root, joinNum);
            if (node == null) {
                return root;
            }
            int prevJoinMeth = node.getJoinType();
            int joinMeth = RandNumb.randInt(0, numJMeth - 1);
            while (joinMeth == prevJoinMeth) {
                joinMeth = RandNumb.randInt(0, numJMeth - 1);
            }
            node.setJoinType(joinMeth);
        }
        return root;
    }

    /**
     * Applies join Commutativity for the join numbered with joinNum
     * *  e.g.,  A X B  is changed as B X A
     * * returns the modifies plan
     **/
    protected Operator neighborCommut(Operator root, int joinNum) {
        System.out.println("------------------neighbor by commutative---------------");
        /** find the node to be altered**/
        Join node = (Join) findNodeAt(root, joinNum);
        if (node == null) {
            return root;
        }
        Operator left = node.getLeft();
        Operator right = node.getRight();
        node.setLeft(right);
        node.setRight(left);
        node.getCondition().flip();
        modifySchema(root);
        return root;
    }

    /**
     * Applies join Associativity for the join numbered with joinNum
     * *  e.g., (A X B) X C is changed to A X (B X C)
     * *  returns the modifies plan
     **/
    protected Operator neighborAssoc(Operator root, int joinNum) {
        /** find the node to be altered**/
        Join op = (Join) findNodeAt(root, joinNum);
        if (op == null) {
            return root;
        }
        Operator left = op.getLeft();
        Operator right = op.getRight();
        if (left.getOpType() == OpType.JOIN && right.getOpType() != OpType.JOIN) {
            transformLefttoRight(op, (Join) left);
        } else if (left.getOpType() != OpType.JOIN && right.getOpType() == OpType.JOIN) {
            transformRighttoLeft(op, (Join) right);
        } else if (left.getOpType() == OpType.JOIN && right.getOpType() == OpType.JOIN) {
            if (RandNumb.flipCoin())
                transformLefttoRight(op, (Join) left);
            else
                transformRighttoLeft(op, (Join) right);
        } else {
            // The join is just A X B,  therefore Association rule is not applicable
        }

        /** modify the schema before returning the root **/
        modifySchema(root);
        return root;
    }

    /**
     * Change the ordering of sort operation
     * @param root
     * @param joinNum
     * @return
     */
    protected Operator neighborSortPosition(Operator root, int joinNum) {
        Operator grandParent = findSortNodeGrandParent(root);
        if (grandParent == null) {
            return root;
        }
        if (grandParent.getBase() == null) {
            return root;
        }
        if (grandParent.getOpType() == OpType.SORT) {
            return root;
        }
        if (grandParent.getOpType() == OpType.JOIN) {
            if (((Join) grandParent).getLeft().getOpType() == OpType.SORT ||
                    ((Join) grandParent).getRight().getOpType() == OpType.SORT) {
                sortMoveDown(grandParent);
            } else {
                sortMoveUp(grandParent);
            }
            modifySchema(root);
            return root;
        }
        if (grandParent.getOpType() == OpType.SELECT ||
                grandParent.getOpType() == OpType.PROJECT ||
                grandParent.getOpType() == OpType.DISTINCT) {
            if (grandParent.getBase().getOpType() == OpType.SORT) {
                sortMoveDown(grandParent);
            } else {
                if (RandNumb.flipCoin()) {
                    sortMoveDown(grandParent.getBase());
                }
                sortMoveUp(grandParent);
            }
            modifySchema(root);
            return root;
        }

        return root;
    }

    protected void sortMoveUp(Operator node) {
        System.out.println("Cannot move up yet");
    }

    protected void sortMoveDown(Operator node) {
        System.out.println("Moving down!!!!!!!!!!!!!");
        if (node.getOpType() == OpType.JOIN) {
            Operator left = ((Join) node).getLeft();
            Operator right = ((Join) node).getRight();
            if (left.getOpType() == OpType.SORT) {
                Operator tmp1 = left.getBase();
                generateNewSortedRunAndSwitchPosition(node, tmp1, true, true);
            }
            if (right.getOpType() == OpType.SORT) {
                Operator tmp2 = left.getBase();
                generateNewSortedRunAndSwitchPosition(node, tmp2, true, false);
            }
        } else {
            Operator child = node.getBase();
            if (child.getOpType() == OpType.SORT) {
                Operator temp = child.getBase();
                generateNewSortedRunAndSwitchPosition(node, temp, false, false);
            }
        }
    }

    private void generateNewSortedRunAndSwitchPosition(Operator node, Operator child, boolean isJoin, boolean isLeft) {
        if (child != null) {
            Operator base = child.getBase();
            Operator newSort;
            if (base == null || base.getOpType() == OpType.SCAN) {
                // Do nothing
            } else if (base.getOpType() == OpType.SELECT) {
                Attribute attr = ((Select) base).getCondition().getLhs();
                int index = base.getSchema().indexOf(attr);
                newSort = new SortedRun(base, BufferManager.numBuffer, index);
                newSort.setSchema(base.getSchema());
                child.setBase(newSort);
                child.setSchema(newSort.getSchema());
                if (isJoin && isLeft) {
                    ((Join) node).setLeft(child);
                } else if (isJoin) {
                    ((Join) node).setRight(child);
                } else {
                    node.setBase(child);
                }
            } else if (base.getOpType() == OpType.JOIN) {
                ArrayList<Integer> indexes = new ArrayList<>();
                if (((Join) base).getConditionList() != null) {
                    for (Condition condition: ((Join) base).getConditionList()) {
                        indexes.add(base.getSchema().indexOf(condition.getLhs()));
                    }
                }
                if (((Join) base).getCondition() != null) {
                    indexes.add(base.getSchema().indexOf(((Join) base).getCondition().getLhs()));
                }
                if (indexes.size() > 0) {
                    newSort = new SortedRun(base, BufferManager.numBuffer, indexes);
                    newSort.setSchema(base.getSchema());
                } else {
                    newSort = new SortedRun(base, BufferManager.numBuffer);
                    newSort.setSchema(base.getSchema());
                }
                child.setBase(newSort);
                child.setSchema(newSort.getSchema());
                if (isJoin && isLeft) {
                    ((Join) node).setLeft(child);
                } else if (isJoin) {
                    ((Join) node).setRight(child);
                } else {
                    node.setBase(child);
                }
            } else if (base.getOpType() == OpType.PROJECT) {
                newSort = new SortedRun(base, BufferManager.numBuffer);
                newSort.setSchema(base.getSchema());
                child.setBase(newSort);
                child.setSchema(newSort.getSchema());
                if (isJoin && isLeft) {
                    ((Join) node).setLeft(child);
                } else if (isJoin) {
                    ((Join) node).setRight(child);
                } else {
                    node.setBase(child);
                }
            }
        }
    }

    /**
     * This is given plan (A X B) X C
     **/
    protected void transformLefttoRight(Join op, Join left) {
        System.out.println("------------------Left to Right neighbor--------------");
        Operator right = op.getRight();
        Operator leftleft = left.getLeft();
        Operator leftright = left.getRight();
        Attribute leftAttr = op.getCondition().getLhs();
        Join temp;

        if (leftright.getSchema().contains(leftAttr)) {
            System.out.println("----------------CASE 1-----------------");
            /** CASE 1 :  ( A X a1b1 B) X b4c4  C     =  A X a1b1 (B X b4c4 C)
             ** a1b1,  b4c4 are the join conditions at that join operator
             **/
            temp = new Join(leftright, right, op.getCondition(), OpType.JOIN);
            temp.setJoinType(op.getJoinType());
            temp.setNodeIndex(op.getNodeIndex());
            op.setLeft(leftleft);
            op.setJoinType(left.getJoinType());
            op.setNodeIndex(left.getNodeIndex());
            op.setRight(temp);
            op.setCondition(left.getCondition());

        } else {
            System.out.println("--------------------CASE 2---------------");
            /**CASE 2:   ( A X a1b1 B) X a4c4  C     =  B X b1a1 (A X a4c4 C)
             ** a1b1,  a4c4 are the join conditions at that join operator
             **/
            temp = new Join(leftleft, right, op.getCondition(), OpType.JOIN);
            temp.setJoinType(op.getJoinType());
            temp.setNodeIndex(op.getNodeIndex());
            op.setLeft(leftright);
            op.setRight(temp);
            op.setJoinType(left.getJoinType());
            op.setNodeIndex(left.getNodeIndex());
            Condition newcond = left.getCondition();
            newcond.flip();
            op.setCondition(newcond);
        }
    }

    protected void transformRighttoLeft(Join op, Join right) {
        System.out.println("------------------Right to Left Neighbor------------------");
        Operator left = op.getLeft();
        Operator rightleft = right.getLeft();
        Operator rightright = right.getRight();
        Attribute rightAttr = (Attribute) op.getCondition().getRhs();
        Join temp;

        if (rightleft.getSchema().contains(rightAttr)) {
            System.out.println("----------------------CASE 3-----------------------");
            /** CASE 3 :  A X a1b1 (B X b4c4  C)     =  (A X a1b1 B ) X b4c4 C
             ** a1b1,  b4c4 are the join conditions at that join operator
             **/
            temp = new Join(left, rightleft, op.getCondition(), OpType.JOIN);
            temp.setJoinType(op.getJoinType());
            temp.setNodeIndex(op.getNodeIndex());
            op.setLeft(temp);
            op.setRight(rightright);
            op.setJoinType(right.getJoinType());
            op.setNodeIndex(right.getNodeIndex());
            op.setCondition(right.getCondition());
        } else {
            System.out.println("-----------------------------CASE 4-----------------");
            /** CASE 4 :  A X a1c1 (B X b4c4  C)     =  (A X a1c1 C ) X c4b4 B
             ** a1b1,  b4c4 are the join conditions at that join operator
             **/
            temp = new Join(left, rightright, op.getCondition(), OpType.JOIN);
            temp.setJoinType(op.getJoinType());
            temp.setNodeIndex(op.getNodeIndex());
            op.setLeft(temp);
            op.setRight(rightleft);
            op.setJoinType(right.getJoinType());
            op.setNodeIndex(right.getNodeIndex());
            Condition newcond = right.getCondition();
            newcond.flip();
            op.setCondition(newcond);
        }
    }

    /**
     * This method traverses through the query plan and
     * * returns the node mentioned by joinNum
     **/
    protected Operator findNodeAt(Operator node, int joinNum) {
        if (node.getOpType() == OpType.JOIN) {
            if (((Join) node).getNodeIndex() == joinNum) {
                return node;
            } else {
                Operator temp;
                temp = findNodeAt(((Join) node).getLeft(), joinNum);
                if (temp == null)
                    temp = findNodeAt(((Join) node).getRight(), joinNum);
                return temp;
            }
        } else if (node.getOpType() == OpType.SCAN) {
            return null;
        } else if (node.getOpType() == OpType.SELECT) {
            // if sort/project/select operator
            return findNodeAt(((Select) node).getBase(), joinNum);
        } else if (node.getOpType() == OpType.PROJECT) {
            return findNodeAt(((Project) node).getBase(), joinNum);
        } else if (node.getOpType() == OpType.SORT) {
            return findNodeAt(((SortedRun) node).getBase(), joinNum);
        } else if (node.getOpType() == OpType.DISTINCT) {
            return findNodeAt(((Distinct) node).getBase(), joinNum);
        } else if (node.getOpType() == OpType.ORDERBY) {
            return findNodeAt(((Orderby) node).getBase(), joinNum);
        } else {
            return null;
        }
    }

    /**
     * This function finds the parent node of the parent node of a sort operator, used for push up sort operations
     * @param node
     * @return
     */
    protected Operator findSortNodeGrandParent(Operator node) {
        if (node.getOpType() == OpType.SCAN) {
            return null;
        }
        if (node.getOpType() == OpType.SORT) {
            return node;
        }
        if (node.getOpType() == OpType.JOIN) {
            Operator temp = findSortNodeGrandParent(((Join) node).getLeft());
            if (temp == null) {
                temp = findSortNodeGrandParent(((Join) node).getRight());
            }
            return temp;
        }
        if (node.getOpType() == OpType.SELECT ||
                node.getOpType() == OpType.PROJECT ||
                node.getOpType() == OpType.DISTINCT) {
            if (node.getBase() == null) {
                return null;
            }
            Operator child = node.getBase();
            if (child.getOpType() == OpType.SCAN) {
                return null;
            }
            if (child.getOpType() == OpType.SORT) {
                return node;
            }
            if (child.getOpType() == OpType.JOIN) {
                if (((Join) child).getLeft().getOpType() == OpType.SORT ||
                        ((Join) child).getRight().getOpType() == OpType.SORT) {
                    return node;
                }
                Operator tmp = findSortNodeGrandParent(((Join) child).getLeft());
                if (tmp == null) {
                    tmp = findSortNodeGrandParent(((Join) child).getRight());
                }
                return tmp;
            }
            if (child.getOpType() == OpType.SELECT ||
                    child.getOpType() == OpType.PROJECT ||
                    child.getOpType() == OpType.DISTINCT) {
                if (child.getBase().getOpType() == OpType.SORT) {
                    return node;
                }
                return findSortNodeGrandParent(child);
            }
        }

        return null;
    }

    /**
     * Modifies the schema of operators which are modified due to selecing an alternative neighbor plan
     **/
    protected void modifySchema(Operator node) {
        // Todo: add other operators
        if (node.getOpType() == OpType.JOIN) {
            Operator left = ((Join) node).getLeft();
            Operator right = ((Join) node).getRight();
            modifySchema(left);
            modifySchema(right);
            node.setSchema(left.getSchema().joinWith(right.getSchema()));
        } else if (node.getOpType() == OpType.SELECT) {
            Operator base = ((Select) node).getBase();
            modifySchema(base);
            node.setSchema(base.getSchema());
        } else if (node.getOpType() == OpType.PROJECT) {
            Operator base = ((Project) node).getBase();
            modifySchema(base);
            ArrayList attrlist = ((Project) node).getProjAttr();
            node.setSchema(base.getSchema().subSchema(attrlist));
        }
    }
}
