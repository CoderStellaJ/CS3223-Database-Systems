package qp.parser;

import qp.utils.SQLQuery;

public class ProcessedQuery {
    private SQLQuery query;
    private int type;

    public ProcessedQuery(SQLQuery query, int type) {
        this.query = query;
        this.type = type;
    }

    public SQLQuery getQuery() {
        return query;
    }

    public int getType() {
        return type;
    }

    public void setQuery(SQLQuery query) {
        this.query = query;
    }

    public void setType(int type) {
        this.type = type;
    }
}
