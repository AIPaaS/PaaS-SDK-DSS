package com.ai.paas.ipaas.dss.exception;

public class DSSRuntimeException extends RuntimeException {
    /**
     * 
     */
    private static final long serialVersionUID = 4661638941581114540L;

    private static final String DSS_MSG = "DSS RUNTIME ERROR";

    public DSSRuntimeException(Exception ex) {
        super(DSS_MSG, ex);
    }

}
