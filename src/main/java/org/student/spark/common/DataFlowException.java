package org.student.spark.common;

public class DataFlowException extends RuntimeException {

    public DataFlowException() {super();}

    public DataFlowException(Throwable cause) {
        super(cause);
    }

    public DataFlowException(String message, Throwable cause) {
        super(message, cause);
    }

    public DataFlowException(String message) {
        super(message);
    }

}
