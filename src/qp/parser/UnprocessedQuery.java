package qp.parser;

public class UnprocessedQuery {
    private String value;
    private int setOperation; // 0 means no set operation, 1 means union, 2 means intersection

    public UnprocessedQuery(String value, int setOperation) {
        this.value = value;
        this.setOperation = setOperation;
    }

    public String getValue() {
        return value;
    }

    public int getSetOperation() {
        return setOperation;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public void setSetOperation(int setOperation) {
        this.setOperation = setOperation;
    }
}
