package qp.utils;

import java.util.ArrayList;
import java.util.Comparator;

public class TupleComparator implements Comparator<Tuple>{

    int leftIndex;
    int rightIndex;
    ArrayList<Integer> leftIndexList;
    ArrayList<Integer> rightIndexList;

    public TupleComparator() {
        leftIndex = -1;
        rightIndex = -1;
    }

    public TupleComparator(int leftIndex, int rightIndex) {
        this.leftIndex = leftIndex;
        this.rightIndex = rightIndex;
    }

    public TupleComparator(ArrayList<Integer> leftIndexList, ArrayList<Integer> rightIndexList) {
        leftIndex = -1;
        rightIndex = -1;
        this.leftIndexList = leftIndexList;
        this.rightIndexList = rightIndexList;
    }

    @Override
    public int compare(Tuple t1, Tuple t2) {
        if (leftIndex != -1) {
            return Tuple.compareTuples(t1, t2, leftIndex, rightIndex);
        }
        if (leftIndexList != null) {
            return Tuple.compareTuples(t1, t2, leftIndexList, rightIndexList);
        }
        return Tuple.compareTuples(t1, t2, 0, 0);
    }
}
