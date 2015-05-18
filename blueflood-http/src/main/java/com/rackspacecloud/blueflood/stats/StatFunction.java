package com.rackspacecloud.blueflood.stats;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.outputs.formats.MetricData.Type;
import com.rackspacecloud.blueflood.types.BasicRollup;
import com.rackspacecloud.blueflood.types.Points;
import com.rackspacecloud.blueflood.types.Points.Point;
import com.rackspacecloud.blueflood.types.SimpleNumber;

public enum StatFunction {

	SUM("sum") {
		@Override
		public MetricData exec(List<MetricData> params, List<Double> constantValues) throws TargetTypeException {
			if (params.isEmpty()) {
				throw new TargetTypeException("Sum requires at least one parameter.");
			}

			if (constantValues != null && constantValues.size() != 0) {
				throw new TargetTypeException("Sum does not require constant parameter.");
			}

			Map<Long, Double> values = new HashMap<Long, Double>();

			Iterator<MetricData> it = params.iterator();
			MetricData firstParam = it.next();
			Map<Long, Point> firstPoints = firstParam.getData().getPoints();

			for (Map.Entry<Long, Point> point : firstPoints.entrySet()) {
				long timestamp = point.getKey() - (point.getKey() % GRANULARITY);

				if (point.getValue().getData() instanceof SimpleNumber) {
					values.put(timestamp, ((Point<SimpleNumber>) point.getValue()).getData().getValue().doubleValue());
				} else if (point.getValue().getData() instanceof BasicRollup) {
					if (((Point<BasicRollup>) point.getValue()).getData().getAverage().isFloatingPoint()) {
						values.put(timestamp, ((Point<BasicRollup>) point.getValue()).getData().getAverage().toDouble());
					} else {
						values.put(timestamp, (double) ((Point<BasicRollup>) point.getValue()).getData().getAverage().toLong());
					}
				} else {
					throw new TargetTypeException("Stats requests expect SimpleNumber data instead of " + point.getClass());
				}
			}

			while (it.hasNext()) {
				MetricData data = it.next();
				Map<Long, Point> dataPoints = data.getData().getPoints();

				for (Map.Entry<Long, Point> point : dataPoints.entrySet()) {
					long timestamp = point.getKey() - (point.getKey() % GRANULARITY);

					Double tmp = values.get(timestamp);
					if (tmp == null) {
						tmp = 0.0;
					}

					if (point.getValue().getData() instanceof SimpleNumber) {
						values.put(timestamp, ((Point<SimpleNumber>) point.getValue()).getData().getValue().doubleValue() + tmp);
					} else if (point.getValue().getData() instanceof BasicRollup) {
						if (((Point<BasicRollup>) point.getValue()).getData().getAverage().isFloatingPoint()) {
							values.put(timestamp, ((Point<BasicRollup>) point.getValue()).getData().getAverage().toDouble() + tmp);
						} else {
							values.put(timestamp, (double) ((Point<BasicRollup>) point.getValue()).getData().getAverage().toLong() + tmp);
						}
					} else {
						throw new TargetTypeException("Stats requests expect SimpleNumber data instead of " + point.getClass());
					}
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
		public MetricData exec(List<MetricData> params, List<Double> constantValues) throws TargetTypeException {
			if (params.isEmpty()) {
				throw new TargetTypeException("Average requires at least one parameter.");
			}

			if (constantValues != null && constantValues.size() != 0) {
				throw new TargetTypeException("Average does not require constant parameter.");
			}

			Map<Long, Double> values = new HashMap<Long, Double>();

			Iterator<MetricData> it = params.iterator();
			MetricData firstParam = it.next();
			Map<Long, Point> firstPoints = firstParam.getData().getPoints();

			for (Map.Entry<Long, Point> point : firstPoints.entrySet()) {
				long timestamp = point.getKey() - (point.getKey() % GRANULARITY);

				if (point.getValue().getData() instanceof SimpleNumber) {
					values.put(timestamp, ((Point<SimpleNumber>) point.getValue()).getData().getValue().doubleValue());
				} else if (point.getValue().getData() instanceof BasicRollup) {
					if (((Point<BasicRollup>) point.getValue()).getData().getAverage().isFloatingPoint()) {
						values.put(timestamp, ((Point<BasicRollup>) point.getValue()).getData().getAverage().toDouble());
					} else {
						values.put(timestamp, (double) ((Point<BasicRollup>) point.getValue()).getData().getAverage().toLong());
					}
				} else {
					throw new TargetTypeException("Stats requests expect SimpleNumber data instead of " + point.getClass());
				}
			}

			while (it.hasNext()) {
				MetricData data = it.next();
				Map<Long, Point> dataPoints = data.getData().getPoints();

				for (Map.Entry<Long, Point> point : dataPoints.entrySet()) {
					long timestamp = point.getKey() - (point.getKey() % GRANULARITY);

					Double tmp = values.get(timestamp);
					if (tmp == null) {
						tmp = 0.0;
					}

					if (point.getValue().getData() instanceof SimpleNumber) {
						values.put(timestamp, ((Point<SimpleNumber>) point.getValue()).getData().getValue().doubleValue() + tmp);
					} else if (point.getValue().getData() instanceof BasicRollup) {
						if (((Point<BasicRollup>) point.getValue()).getData().getAverage().isFloatingPoint()) {
							values.put(timestamp, ((Point<BasicRollup>) point.getValue()).getData().getAverage().toDouble() + tmp);
						} else {
							values.put(timestamp, (double) ((Point<BasicRollup>) point.getValue()).getData().getAverage().toLong() + tmp);
						}
					} else {
						throw new TargetTypeException("Stats requests expect SimpleNumber data instead of " + point.getClass());
					}
				}
			}

			Points<SimpleNumber> resultPoints = new Points<SimpleNumber>();
			for (Map.Entry<Long, Double> value : values.entrySet()) {
				resultPoints.add(new Point<SimpleNumber>(value.getKey(), new SimpleNumber(value.getValue() / params.size())));
			}

			MetricData result = new MetricData(resultPoints, "", Type.NUMBER);
			return result;
		}

	},
	DERIVATIVE("derivative") {

		@Override
		public MetricData exec(List<MetricData> params, List<Double> constantValues) throws TargetTypeException {
			if (params.size() != 1) {
				throw new TargetTypeException("Derivative requires one parameter.");
			}
			if (constantValues != null && constantValues.size() != 0) {
				throw new TargetTypeException("Derivative does not require constant parameter.");
			}

			Map<Long, Point> points = params.get(0).getData().getPoints();

			Number prev = null;
			Long prevKey = null;
			Points<SimpleNumber> resultPoints = new Points<SimpleNumber>();
			for (Map.Entry<Long, Point> point : points.entrySet()) {
				if (prev != null) {
					double deltaT = (point.getKey() - prevKey) / GRANULARITY;
					if (point.getValue().getData() instanceof SimpleNumber) {
						Number tmp = ((Point<SimpleNumber>) point.getValue()).getData().getValue();
						resultPoints.add(new Point<SimpleNumber>(point.getKey(), new SimpleNumber((tmp.doubleValue() - prev.doubleValue()) / deltaT)));
						prev = tmp;
					} else if (point.getValue().getData() instanceof BasicRollup) {
						if (((Point<BasicRollup>) point.getValue()).getData().getAverage().isFloatingPoint()) {
							Number tmp = ((Point<BasicRollup>) point.getValue()).getData().getAverage().toDouble();
							resultPoints.add(new Point<SimpleNumber>(point.getKey(), new SimpleNumber((tmp.doubleValue() - prev.doubleValue()) / deltaT)));
							prev = tmp;
						} else {
							Number tmp = ((Point<BasicRollup>) point.getValue()).getData().getAverage().toLong();
							resultPoints.add(new Point<SimpleNumber>(point.getKey(), new SimpleNumber((tmp.doubleValue() - prev.doubleValue()) / deltaT)));
							prev = tmp;
						}
					} else {
						throw new TargetTypeException("Stats requests expect SimpleNumber data instead of " + point.getClass());
					}
					prevKey = point.getKey();
				} else {
					prevKey = point.getKey();
					if (point.getValue().getData() instanceof SimpleNumber) {
						prev = ((Point<SimpleNumber>) point.getValue()).getData().getValue();
					} else if (point.getValue().getData() instanceof BasicRollup) {
						if (((Point<BasicRollup>) point.getValue()).getData().getAverage().isFloatingPoint()) {
							prev = ((Point<BasicRollup>) point.getValue()).getData().getAverage().toDouble();
						} else {
							prev = ((Point<BasicRollup>) point.getValue()).getData().getAverage().toLong();
						}
					} else {
						throw new TargetTypeException("Stats requests expect SimpleNumber data instead of " + point.getClass());
					}
				}
			}

			MetricData result = new MetricData(resultPoints, "", Type.NUMBER);
			return result;
		}

	},
	NONNEGATIVEDERIVATIVE("nonNegativeDerivative") {

		@Override
		public MetricData exec(List<MetricData> params, List<Double> constantValues) throws TargetTypeException {
			if (params.size() != 1) {
				throw new TargetTypeException("Derivative requires two parameter.");
			}

			Map<Long, Point> points = params.get(0).getData().getPoints();

			Double maxValue = null;
			if (constantValues != null && constantValues.size() != 0) {
				maxValue = constantValues.get(0);
			}

			Number prev = null;
			Long prevKey = null;
			Points<SimpleNumber> resultPoints = new Points<SimpleNumber>();
			for (Map.Entry<Long, Point> point : points.entrySet()) {
				if (prev != null) {
					double deltaT = (point.getKey() - prevKey) / GRANULARITY;
					Number tmp = null;

					if (point.getValue().getData() instanceof SimpleNumber) {
						tmp = ((Point<SimpleNumber>) point.getValue()).getData().getValue();
					} else if (point.getValue().getData() instanceof BasicRollup) {
						if (((Point<BasicRollup>) point.getValue()).getData().getAverage().isFloatingPoint()) {
							tmp = ((Point<BasicRollup>) point.getValue()).getData().getAverage().toDouble();
						} else {
							tmp = ((Point<BasicRollup>) point.getValue()).getData().getAverage().toLong();
						}
					} else {
						throw new TargetTypeException("Stats requests expect SimpleNumber data instead of " + point.getClass());
					}

					double diff = tmp.doubleValue() - prev.doubleValue();
					if (diff >= 0) {
						resultPoints.add(new Point<SimpleNumber>(point.getKey(), new SimpleNumber(diff / deltaT)));
					} else if (maxValue != null && maxValue >= tmp.doubleValue()) {
						resultPoints.add(new Point<SimpleNumber>(point.getKey(), new SimpleNumber(maxValue - prev.doubleValue() + tmp.doubleValue() + 1)));
					}

					prevKey = point.getKey();
					prev = tmp;
				} else {
					prevKey = point.getKey();
					if (point.getValue().getData() instanceof SimpleNumber) {
						prev = ((Point<SimpleNumber>) point.getValue()).getData().getValue();
					} else if (point.getValue().getData() instanceof BasicRollup) {
						if (((Point<BasicRollup>) point.getValue()).getData().getAverage().isFloatingPoint()) {
							prev = ((Point<BasicRollup>) point.getValue()).getData().getAverage().toDouble();
						} else {
							prev = ((Point<BasicRollup>) point.getValue()).getData().getAverage().toLong();
						}
					} else {
						throw new TargetTypeException("Stats requests expect SimpleNumber data instead of " + point.getClass());
					}
				}
			}

			MetricData result = new MetricData(resultPoints, "", Type.NUMBER);
			return result;
		}

	},
	ASPERCENT("asPercent") {

		@Override
		public MetricData exec(List<MetricData> params, List<Double> constantValues) throws TargetTypeException {
			if (params.size() != 2) {
				throw new TargetTypeException("AsPercent requires two parameters.");
			}
			if (constantValues != null && constantValues.size() != 0) {
				throw new TargetTypeException("AsPercent does not require constant parameters.");
			}

			Map<Long, Double> values = new HashMap<Long, Double>();

			Map<Long, Point> firstSeriePoints = params.get(0).getData().getPoints();

			for (Map.Entry<Long, Point> point : firstSeriePoints.entrySet()) {
				long timestamp = point.getKey() - (point.getKey() % GRANULARITY);

				if (point.getValue().getData() instanceof SimpleNumber) {
					values.put(timestamp, ((Point<SimpleNumber>) point.getValue()).getData().getValue().doubleValue());
				} else if (point.getValue().getData() instanceof BasicRollup) {
					if (((Point<BasicRollup>) point.getValue()).getData().getAverage().isFloatingPoint()) {
						values.put(timestamp, ((Point<BasicRollup>) point.getValue()).getData().getAverage().toDouble());
					} else {
						values.put(timestamp, (double) ((Point<BasicRollup>) point.getValue()).getData().getAverage().toLong());
					}
				} else {
					throw new TargetTypeException("Stats requests expect SimpleNumber data instead of " + point.getClass());
				}
			}

			Points<SimpleNumber> resultPoints = new Points<SimpleNumber>();

			Map<Long, Point> secondSeriePoints = params.get(1).getData().getPoints();
			for (Map.Entry<Long, Point> point : secondSeriePoints.entrySet()) {
				long timestamp = point.getKey() - (point.getKey() % GRANULARITY);
				Double val = values.get(timestamp);

				if (val == null) {
					val = 0.0;
				}

				double tmp = 0;
				val *= 100;
				if (point.getValue().getData() instanceof SimpleNumber) {
					tmp = ((Point<SimpleNumber>) point.getValue()).getData().getValue().doubleValue();
				} else if (point.getValue().getData() instanceof BasicRollup) {
					if (((Point<BasicRollup>) point.getValue()).getData().getAverage().isFloatingPoint()) {
						tmp = ((Point<BasicRollup>) point.getValue()).getData().getAverage().toDouble();
					} else {
						tmp = ((Point<BasicRollup>) point.getValue()).getData().getAverage().toLong();
					}
				} else {
					throw new TargetTypeException("Stats requests expect SimpleNumber data instead of " + point.getClass());
				}

				if (tmp != 0) {
					resultPoints.add(new Point<SimpleNumber>(timestamp, new SimpleNumber(val / tmp)));
				}

			}

			MetricData result = new MetricData(resultPoints, "", Type.NUMBER);
			return result;
		}

	};

	private String function;
	private static final Map<String, StatFunction> stringToEnum = new HashMap<String, StatFunction>();
	private final static int GRANULARITY = 60000;

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

	public abstract MetricData exec(List<MetricData> params, List<Double> constantValues) throws TargetTypeException;

}
