package org.cascading.json;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A function that extract data from a JSON object.
 * 
 * @author <a href="mailto:gmarabout@gmail.com">Grégoire Marabout</a>
 */
public class JSONSplitter extends BaseOperation implements Function {

    private LinkedHashMap orderedMap;

    public JSONSplitter(Fields fieldDeclaration){
        super( fieldDeclaration );
        orderedMap = createLinkedMap();
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall){
        String str = (String) functionCall.getArguments().get( 0 );
        JSONObject jsonObject = (JSONObject) JSONSerializer.toJSON( str );

        Tuple output = createOutput( new LinkedHashMap( this.orderedMap ), jsonObject );

        functionCall.getOutputCollector().add( output );
    }

    protected LinkedHashMap createLinkedMap(){
        LinkedHashMap orderedMap = new LinkedHashMap();
        for ( int i = 0; i < getFieldDeclaration().size(); i++ ) {
            orderedMap.put( getFieldDeclaration().get( i ), null );
        }
        return orderedMap;
    }

    protected Tuple createOutput(LinkedHashMap orderedMap, Map map){
        Tuple output = new Tuple();
        Iterator keysIter = map.keySet().iterator();
        while ( keysIter.hasNext() ) {
            Object key = keysIter.next();
            Object value = map.get( key );
            if ( orderedMap.containsKey( key ) )
                orderedMap.put( key, value );
        }
        for ( Iterator<Map.Entry> iter = orderedMap.entrySet().iterator(); iter.hasNext();)
            output.add( iter.next().getValue().toString() );  
        return output;
    }


}
