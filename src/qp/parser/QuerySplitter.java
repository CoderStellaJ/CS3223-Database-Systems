package qp.parser;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

public class QuerySplitter {

    private String originalQuery;
    private List<UnprocessedQuery> queries;

    public QuerySplitter(String originalQuery) {
        this.originalQuery = originalQuery;
        this.queries = new ArrayList<>();
    }

    public void splitQueries() {
        String[] queryStrings = originalQuery.split("INTERSECTION|UNION");
        PriorityQueue<Operation> pq = new PriorityQueue<>(new Comparator<Operation>() {
            @Override
            public int compare(Operation o1, Operation o2) {
                return o1.index - o2.index;
            }
        });
        int index = originalQuery.indexOf("UNION");
        while (index >= 0) {
            Operation op = new Operation(1, index);
            pq.add(op);
            index = originalQuery.indexOf("UNION", index + 1);
        }
        index = originalQuery.indexOf("INTERSECTION");
        while (index >= 0) {
            Operation op = new Operation(2, index);
            pq.add(op);
            index = originalQuery.indexOf("INTERSECTION", index + 1);
        }

        List<UnprocessedQuery> queries = new ArrayList<>();
        for (String queryString: queryStrings) {
            if (pq.isEmpty()) {
                this.queries.add(new UnprocessedQuery(queryString, 0));
            } else {
                this.queries.add(new UnprocessedQuery(queryString, pq.poll().type));
            }
        }
    }

    public List<UnprocessedQuery> getQueries() {
        return this.queries;
    }
}

class Operation {
    int type; // 1 for union, 2 for intersection
    int index;

    public Operation(int type, int index) {
        this.type = type;
        this.index = index;
    }

    public int getType() {
        return type;
    }

    public int getIndex() {
        return index;
    }

    public void setType(int type) {
        this.type = type;
    }

    public void setIndex(int index) {
        this.index = index;
    }
}