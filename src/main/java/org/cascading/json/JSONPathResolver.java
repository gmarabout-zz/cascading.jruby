package org.cascading.json;

import net.sf.json.JSONObject;

import java.io.Serializable;

/**
 * @author <a href="mailto:gmarabout@gmail.com">Grégoire Marabout</a>
 */
public interface JSONPathResolver extends Serializable
{
    Object resolve(JSONObject object, String path);
}
