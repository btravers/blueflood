package com.rackspacecloud.blueflood.stats;

import java.util.Iterator;
import java.util.Map;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.rackspacecloud.blueflood.exceptions.SerializationException;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.types.BasicRollup;
import com.rackspacecloud.blueflood.types.Points.Point;
import com.rackspacecloud.blueflood.types.SimpleNumber;


public class ResponseSerializer {

	public static JsonElement serializeResponse(Map<Target, MetricData> results) throws SerializationException {
		JsonArray jsonResult = new JsonArray();
		
		for (Map.Entry<Target, MetricData> serie : results.entrySet()) {
			jsonResult.add(serializeSerie(serie.getKey(), serie.getValue()));
		}

		return jsonResult;
	}
	
	private static JsonObject serializeSerie(Target target, MetricData datapoints) throws SerializationException {
		JsonObject serie = new JsonObject();
		
		serie.add("target", new JsonPrimitive(serializeTargetName(target)));
		serie.add("datapoints", serializeDatapoints(datapoints));
		
		return serie;
	}
	
	private static String serializeTargetName(Target target) {
		if (target.isMetric()) {
			return target.getTenantId() + ":" + target.getMetricName();
		} else if (target.isFunction()) {
			String name = target.getName() + "(";
			
			Iterator<Target> it = target.getParameters().iterator();			
			while (it.hasNext()) {
				name += serializeTargetName(it.next());
				
				if (it.hasNext()) {
					name += ", ";
				}
			}
			
			name += ")";
			return name;
		} else {
			return target.getConstantParam().toString();
		}
	}
	
	private static JsonArray serializeDatapoints(MetricData datapoints) throws SerializationException {
		JsonArray res = new JsonArray();
		
		Map<Long, Point> points = datapoints.getData().getPoints();
		for (Map.Entry<Long, Point> p : points.entrySet()) {
			JsonArray serializePoint = new JsonArray();
					
			if (p.getValue().getData() instanceof SimpleNumber) {
				SimpleNumber number = (SimpleNumber) p.getValue().getData();
				serializePoint.add(new JsonPrimitive(number.getValue()));
			} else if (p.getValue().getData() instanceof BasicRollup) {
				BasicRollup number = (BasicRollup) p.getValue().getData();
				if (number.getAverage().isFloatingPoint()) {
					serializePoint.add(new JsonPrimitive(number.getAverage().toDouble()));
				} else {
					serializePoint.add(new JsonPrimitive(number.getAverage().toLong()));
				}
				
			} else {
				throw new SerializationException("Unexpected datatype. " + p.getValue().getData().getClass());
			}
			serializePoint.add(new JsonPrimitive(p.getKey()));
			
			res.add(serializePoint);
		}
		
		return res;
	}
}
