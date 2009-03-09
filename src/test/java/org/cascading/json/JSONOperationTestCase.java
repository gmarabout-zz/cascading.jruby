package org.cascading.json;

import junit.framework.TestCase;
import net.sf.json.JSONObject;

/**
 * @author <a href="mailto:gmarabout@gmail.com">Grégoire Marabout</a>
 */
public class JSONOperationTestCase extends TestCase {

    private static String JSON_DATA = "{ name: \"Grégoire\", data: { age: 33, address: { city: \"Paris\", street:\"Sibuet\"} }}";


    public void testDefaultJSONPathResolver() {
        JSONPathResolver resolver = new JSONOperation.DefaultJSONPathResolver(":");
        JSONObject jsonObject = JSONObject.fromObject( JSON_DATA );
        assertEquals("Paris", resolver.resolve( jsonObject, "data:address:city" ));
        assertEquals("Grégoire", resolver.resolve( jsonObject, "name" ));
        assertNull(resolver.resolve(jsonObject, "toto:titi:tata"));
        assertNull(resolver.resolve(jsonObject, "data2:address:zip"));
    }

}
