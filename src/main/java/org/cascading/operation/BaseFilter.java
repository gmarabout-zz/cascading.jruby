package org.cascading.operation;

import cascading.flow.FlowProcess;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;

import java.io.Serializable;

/**
 * @author <a href="mailto:gmarabout@gmail.com">Grégoire Marabout</a>
 */
public class BaseFilter implements Filter, Serializable {
    private Fields fieldDeclaration;

    public BaseFilter(Fields fieldDeclaration){
        this.fieldDeclaration = fieldDeclaration;
    }

    public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall){
        return false;
    }

    public void prepare(FlowProcess flowProcess, OperationCall operationCall){
    }

    public void cleanup(FlowProcess flowProcess, OperationCall operationCall){
    }

    public Fields getFieldDeclaration(){
        return this.fieldDeclaration;
    }

    public int getNumArgs(){
        return 0;
    }
}
