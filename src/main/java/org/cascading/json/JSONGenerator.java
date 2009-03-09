package org.cascading.json;

import cascading.flow.FlowProcess;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleException;
import net.sf.json.JSONObject;

/**
 * @author <a href="mailto:gmarabout@gmail.com">Grégoire Marabout</a>
 */
public class JSONGenerator extends JSONOperation implements Function {
  
    public JSONGenerator(Fields fieldDeclaration, String... paths){
        super( fieldDeclaration, paths );
    }

    public void operate(FlowProcess flowProcess, FunctionCall functionCall){
        JSONObject jsonObject = (JSONObject) functionCall.getArguments().get( 0 );
        Tuple output = new Tuple();
        for ( String path : getPaths() ) {
            Comparable value = getValue( jsonObject, path );
            output.add( value );
        }
        functionCall.getOutputCollector().add( output );
    }
}
