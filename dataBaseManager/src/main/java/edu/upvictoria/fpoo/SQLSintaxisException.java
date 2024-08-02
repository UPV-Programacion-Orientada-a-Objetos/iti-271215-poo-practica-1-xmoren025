package edu.upvictoria.fpoo;

public class SQLSintaxisException extends Exception {
    // Constructor sin argumentos
    public SQLSintaxisException() {
        super();
    }

    // Constructor con mensaje
    public SQLSintaxisException(String message) {
        super(message);
    }
}