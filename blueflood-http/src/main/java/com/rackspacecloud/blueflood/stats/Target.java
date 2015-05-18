package com.rackspacecloud.blueflood.stats;

import java.util.List;

import com.rackspacecloud.blueflood.types.Locator;

public class Target {
	
	private String name;
	private List<Target> parameters;
	private String tenantId;
	private String metricName;
	private Double constantParam;
	
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
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public List<Target> getParameters() {
		return parameters;
	}
	
	public void setParameters(List<Target> parameters) {
		this.parameters = parameters;
	}
	
	public Double getConstantParam() {
		return constantParam;
	}
	
	public void setConstantParam(Double constantParam) {
		this.constantParam = constantParam;
	}
	
	public boolean isMetric() {
		return tenantId != null;
	}
	
	public boolean isFunction() {
		return name != null;
	}
	
	public boolean isConstantValue() {
		return constantParam != null;
	}
	
	public boolean isValidMetric() {
		if (this.metricName != null && this.tenantId != null) {
			return true;
		} else {
			return false;
		}
	}
	
	public boolean isValidFunction() {
		if (this.name != null && this.parameters != null && !this.parameters.isEmpty()) {
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
