package org.cascading.pig;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import net.sf.json.JSONObject;

import java.util.StringTokenizer;

/**
 * 
 * @author <a href="mailto:gmarabout@gmail.com">Grégoire Marabout</a>
 */
public class PigMapParser extends BaseOperation implements Function {

    public PigMapParser(Fields fieldDeclaration){
        super( fieldDeclaration );
    }

    public void operate(FlowProcess flowProcess, FunctionCall functionCall){
        String input = (String) functionCall.getArguments().get( 0 );

        assert input.startsWith( "[" ) && input.endsWith( "]" );

        input = input.substring( 1, input.length() -1 );

        Tuple output = new Tuple( getJSONObject( input ) );
        functionCall.getOutputCollector().add( output );
    }


    private JSONObject getJSONObject(String input){
        JSONObject jsonObject = new JSONObject();
        StringTokenizer tokenizer = new StringTokenizer( input, "," );
        while ( tokenizer.hasMoreTokens() ) {
            String entry = tokenizer.nextToken();
            int index = entry.indexOf( "#" );
            String key = entry.substring( 0, index );
            String value = entry.substring( index + 1 );
            jsonObject.put( key, value );
        }

        return jsonObject;
    }
}