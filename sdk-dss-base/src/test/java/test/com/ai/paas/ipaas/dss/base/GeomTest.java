package test.com.ai.paas.ipaas.dss.base;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

import com.ai.paas.ipaas.dss.IDSSClient;
import com.ai.paas.ipaas.dss.impl.DSSClient;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class GeomTest {

	public static void main(String[] args) {
		IDSSClient dssClient = new DSSClient("10.1.234.150:37017", "dss001", "dss001user", "dss001pwd", "gishn01");
		String str = "{\"type\":\"Polygon\",\"length\":\"4\",\"coordinates\":[[[110.520566,19.843428],[110.486359,19.826834],[110.466237,19.857028],[110.466237,19.857028],[110.520566,19.843428]]],\"logo\":\"CMC\"}";
		Gson gson = new Gson();
		List<double[]> values = new ArrayList<>();
		JsonObject json = gson.fromJson(str, JsonObject.class);
		JsonArray coors = json.get("coordinates").getAsJsonArray();
		// 此时是5个大小的数组
		JsonArray a = coors.get(0).getAsJsonArray();
		for (int i = 0; i < a.size(); i++) {
			JsonArray item = a.get(i).getAsJsonArray();
			
			double[] arr = new double[2];
			arr[0]=item.get(0).getAsDouble();
			arr[1]=item.get(1).getAsDouble();
			values.add(arr);
		}
		List<Document> documents = dssClient.withinPolygon("geom", values);
		System.out.println(documents);
	}

}
