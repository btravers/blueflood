package com.rackspacecloud.blueflood.stats;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.rackspacecloud.blueflood.exceptions.SerializationException;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.outputs.handlers.MetricDataQueryInterface;
import com.rackspacecloud.blueflood.outputs.handlers.RollupHandler;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.Resolution;

public class RollupRequest extends RollupHandler implements MetricDataQueryInterface<MetricData> {

	private Query query;
	private boolean isResolution;

	public RollupRequest(Query q) {
		this.query = q;
	}

	@Override
	public MetricData GetDataByPoints(String tenantId, String metric, long from, long to, int points) throws SerializationException {
		rollupsByPointsMeter.mark();
		Granularity g = Granularity.granularityFromPointsInInterval(from, to, points);
		return getRollupByGranularity(tenantId, metric, from, to, g);
	}

	@Override
	public MetricData GetDataByResolution(String tenantId, String metric, long from, long to, Resolution resolution) throws SerializationException {
		rollupsByGranularityMeter.mark();
		if (resolution == null) {
			resolution = Resolution.FULL;
		}
		Granularity g = Granularity.granularities()[resolution.getValue()];
		return getRollupByGranularity(tenantId, metric, from, to, g);
	}

	public Map<Target, MetricData> getData() throws TargetTypeException, SerializationException {
		if (this.query.getPoints() == 0) {
			this.isResolution = true;
		} else {
			this.isResolution = false;
		}
		Map<Target, MetricData> result = new HashMap<Target, MetricData>();

		for (Target t : this.query.getTargets()) {
			result.put(t, this.getData(t));
		}
		return result;
	}

	public MetricData getData(Target t) throws TargetTypeException, SerializationException {
		if (t.isMetric()) {
			if (t.isValidMetric()) {
				if (this.isResolution) {
					return GetDataByResolution(t.getTenant(), t.getName(), this.query.getFrom(), this.query.getTo(), this.query.getResolution());
				} else {
					return GetDataByPoints(t.getTenant(), t.getName(), this.query.getFrom(), this.query.getTo(), this.query.getPoints());
				}
			} else {
				throw new TargetTypeException("Invalid target.");
			}
		} else if (t.isFunction()) {
			if (t.isValidFunction()) {
				List<MetricData> params = new ArrayList<MetricData>();
				List<Double> constantValues = new ArrayList<Double>();
				for (Target param : t.getParameters()) {
					if (param.isConstantValue()) {
						constantValues.add(param.getConstant());
					} else {
						params.add(this.getData(param));
					}
				}
				StatFunction function = StatFunction.fromString(t.getFunctionName());
				if (function == null) {
					throw new TargetTypeException("Unexpected function.");
				}
				return function.exec(params, constantValues);
			} else {
				throw new TargetTypeException("Invalid target.");
			}
		} else {
			throw new TargetTypeException("Invalid target.");
		}
	}

}
