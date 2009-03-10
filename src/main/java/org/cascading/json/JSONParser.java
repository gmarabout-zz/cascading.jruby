package org.cascading.json;

import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.flow.FlowProcess;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import net.sf.json.JSONObject;

/**
 * A class to parse a JSON string and outputs a JSON object.
 * 
 * @author <a href="mailto:gmarabout@gmail.com">Grégoire Marabout</a>
 */
public class JSONParser extends BaseOperation implements Function {

    public JSONParser(Fields fieldDeclaration){
        super( fieldDeclaration );    
    }

    public void operate(FlowProcess flowProcess, FunctionCall functionCall){
        String input = functionCall.getArguments().getString( 0 );
        JSONObject jsonObject = JSONObject.fromObject( input );
        functionCall.getOutputCollector().add( new Tuple( jsonObject ) );
    }
}
