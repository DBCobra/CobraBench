package bench;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Vector;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Tables {
	public static ObjectMapper om = new ObjectMapper();

	public static HashMap<String, Object> decodeTable(String json) {
		if (json == null) {
			return null;
		}
		try {
			HashMap<String, Object> result = om.readValue(json, HashMap.class);
			return result;
		} catch (JsonParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;

	}

	public static String encodeTable(HashMap<String, Object> t) {
		try {
			String result = om.writeValueAsString(t);
			return result;
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public static String encodeKey(String tableName, String keys[]) {
		String ret = tableName;
		for (int i = 0; i < keys.length; i++) {
			ret += ":" + keys[i];
		}
		return ret;
	}

	public static String encodeKey(String tableName, int keys[]) {
		String ret = tableName;
		for (int i = 0; i < keys.length; i++) {
			ret += ":" + Integer.toString(keys[i]);
		}
		return ret;
	}

	public static String encodeList(ArrayList<String> l) {
		try {
			String result = om.writeValueAsString(l);
			return result;
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public static ArrayList<String> decodeList(String json) {
		if (json == null) {
			return new ArrayList<String>();
		}
		try {
			ArrayList<String> result = om.readValue(json, ArrayList.class);
			return result;
		} catch (JsonParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

}
