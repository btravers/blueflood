package com.rackspacecloud.blueflood.stats;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.outputs.formats.MetricData.Type;
import com.rackspacecloud.blueflood.types.Points;
import com.rackspacecloud.blueflood.types.Points.Point;
import com.rackspacecloud.blueflood.types.SimpleNumber;

public enum StatFunction {
	
	SUM("sum") {
		@Override
        public MetricData exec(List<MetricData> params) throws TargetTypeException {
			if (params.isEmpty()) {
				throw new TargetTypeException("Sum require at least one parameter.");
			}
			
			Map<Long, Double> values = new HashMap<Long, Double>();
			
			Iterator<MetricData> it = params.iterator();
			MetricData firstParam = it.next();
			Map<Long, Point> firstPoints = firstParam.getData().getPoints();
			
			for (Map.Entry<Long, Point> point : firstPoints.entrySet()) {
				values.put(point.getKey(), ((Point<SimpleNumber>) point.getValue()).getData().getValue().doubleValue());
			}
			
			while (it.hasNext()) {
				MetricData data = it.next();
				Map<Long, Point> dataPoints = data.getData().getPoints();
				
				for (Map.Entry<Long, Point> point : dataPoints.entrySet()) {
					Double tmp = values.get(point.getKey());
					values.put(point.getKey(),((Point<SimpleNumber>) point.getValue()).getData().getValue().doubleValue() + tmp);
				}
			}
			
			Points<SimpleNumber> resultPoints = new Points<SimpleNumber>();
			for (Map.Entry<Long, Double> value : values.entrySet()) {
				resultPoints.add(new Point<SimpleNumber>(value.getKey(), new SimpleNumber(value.getValue())));
			}
			
			MetricData result = new MetricData(resultPoints, "", Type.NUMBER);
			return result;
        }
	},
	AVERAGE("average") {

		@Override
		public MetricData exec(List<MetricData> params) throws TargetTypeException {
			if (params.isEmpty()) {
				throw new TargetTypeException("Sum require at least one parameter.");
			}
			
			Map<Long, Double> values = new HashMap<Long, Double>();
			
			Iterator<MetricData> it = params.iterator();
			MetricData firstParam = it.next();
			Map<Long, Point> firstPoints = firstParam.getData().getPoints();
			
			for (Map.Entry<Long, Point> point : firstPoints.entrySet()) {
				values.put(point.getKey(), ((Point<SimpleNumber>) point.getValue()).getData().getValue().doubleValue());
			}
			
			while (it.hasNext()) {
				MetricData data = it.next();
				Map<Long, Point> dataPoints = data.getData().getPoints();
				
				for (Map.Entry<Long, Point> point : dataPoints.entrySet()) {
					
					Double tmp = values.get(point.getKey());
					values.put(point.getKey(),((Point<SimpleNumber>) point.getValue()).getData().getValue().doubleValue() + tmp);
				}
			}
			
			Points<SimpleNumber> resultPoints = new Points<SimpleNumber>();
			for (Map.Entry<Long, Double> value : values.entrySet()) {
				resultPoints.add(new Point<SimpleNumber>(value.getKey(), new SimpleNumber(value.getValue()/params.size())));
			}
			
			MetricData result = new MetricData(resultPoints, "", Type.NUMBER);
			return result;
		}
		
	};
	
	private String function;
	private static final Map<String, StatFunction> stringToEnum = new HashMap<String, StatFunction>();
	
	private StatFunction(String s) {
		this.function = s;
	}
	
	static {
        for (StatFunction sf : values()) {
            stringToEnum.put(sf.toString().toLowerCase(), sf);
        }
    }
	
	public static StatFunction fromString(String s) {
        return stringToEnum.get(s.toLowerCase());
    }
	
	@Override
    public String toString() {
        return this.function;
    }

	public abstract MetricData exec(List<MetricData> params) throws TargetTypeException;

}
