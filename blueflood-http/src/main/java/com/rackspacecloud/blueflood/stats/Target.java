package com.rackspacecloud.blueflood.stats;

import java.util.List;

import com.google.gson.annotations.SerializedName;
import com.rackspacecloud.blueflood.types.Locator;

public class Target {

	@SerializedName("function")
	private String functionName;
	private List<Target> parameters;
	private String tenant;
	private String name;
	private Double constant;

	public String getTenant() {
		return tenant;
	}

	public void setTenant(String tenant) {
		this.tenant = tenant;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getFunctionName() {
		return functionName;
	}

	public void setFunctionName(String functionName) {
		this.functionName = functionName;
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
		return tenant != null;
	}

	public boolean isFunction() {
		return functionName != null;
	}

	public boolean isConstantValue() {
		return constant != null;
	}

	public boolean isValidMetric() {
		if (this.name != null && this.tenant != null) {
			return true;
		} else {
			return false;
		}
	}

	public boolean isValidFunction() {
		if (this.functionName != null && this.parameters != null && !this.parameters.isEmpty()) {
			return true;
		} else {
			return false;
		}
	}

	public Locator getLocator() throws TargetTypeException {
		if (this.isMetric()) {
			throw new TargetTypeException("Expecting a metric");
		}
		return Locator.createLocatorFromPathComponents(tenant, name);
	}
}
