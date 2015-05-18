package com.rackspacecloud.blueflood.stats;

import java.util.List;

import com.google.gson.annotations.SerializedName;
import com.rackspacecloud.blueflood.types.Locator;

public class Target {

	private String function;
	private List<Target> parameters;
	private String tenantId;
	private String metricName;
	private Double constant;
	
	public String getTenantId() {
		return tenantId;
	}
	
	public void setTenantId(String tenantId) {
		this.tenantId = tenantId;
	}
	
	public String getMetricName() {
		return metricName;
	}
	
	public void setMetricName(String metricName) {
		this.metricName = metricName;
	}
	
	public String getFunction() {
		return function;
	}
	
	public void setFunction(String function) {
		this.function = function;
	}
	
	public List<Target> getParameters() {
		return parameters;
	}
	
	public void setParameters(List<Target> parameters) {
		this.parameters = parameters;
	}
	
	public Double getConstant() {
		return constant;
	}
	
	public void setConstant(Double constant) {
		this.constant = constant;
	}
	
	public boolean isMetric() {
		return tenantId != null;
	}
	
	public boolean isFunction() {
		return function != null;
	}
	
	public boolean isConstantValue() {
		return constant != null;
	}
	
	public boolean isValidMetric() {
		if (this.metricName != null && this.tenantId != null) {
			return true;
		} else {
			return false;
		}
	}
	
	public boolean isValidFunction() {
		if (this.function != null && this.parameters != null && !this.parameters.isEmpty()) {
			return true;
		} else {
			return false;
		}
	}
	
	public Locator getLocator() throws TargetTypeException {
		if (this.isMetric()) {
			throw new TargetTypeException("Expecting a metric");
		}
		return Locator.createLocatorFromPathComponents(tenantId, metricName);
	}
}
