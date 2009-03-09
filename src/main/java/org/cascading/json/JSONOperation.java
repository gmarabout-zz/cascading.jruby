package org.cascading.json;

import cascading.operation.BaseOperation;
import cascading.tuple.Fields;
import net.sf.json.JSONObject;

/**
 * @author <a href="mailto:gmarabout@gmail.com">Grégoire Marabout</a>
 */
public class JSONOperation extends BaseOperation {
    private String[] paths;
    private JSONPathResolver resolver;

    protected JSONOperation(Fields fieldDeclaration, String...paths){
       this(fieldDeclaration, new DefaultJSONPathResolver(":"), paths);
    }

    protected JSONOperation(Fields fieldDeclaration, JSONPathResolver resolver, String...paths){
        super( fieldDeclaration );
        this.paths = paths;
        this.resolver = resolver;
    }

    protected String[] getPaths() {
        return paths;
    }

    protected Comparable getValue(JSONObject jsonObject, String path) {
        Object value = resolver.resolve( jsonObject, path );
        if (value instanceof Comparable)
            return (Comparable) value;
        return null;
    }

    /**
     * Default JSON path resolver
     */
    public static class DefaultJSONPathResolver implements JSONPathResolver {
        private String pathSeparator;

        public DefaultJSONPathResolver(String pathSeparator) {
           this.pathSeparator = pathSeparator;
        }

        public Object resolve(JSONObject object, String path){
            int index = path.lastIndexOf( pathSeparator );
            Object value = object.get( path );
            if (value == null && index > 0) {
                String subpath = path.substring( 0, index );
                String key = path.substring( index+1 );
                Object subValue = resolve( object, subpath);
                if (subValue instanceof JSONObject) {
                    return ((JSONObject) subValue).get(key);
                }
            }
            return value;
        }
    }

}
