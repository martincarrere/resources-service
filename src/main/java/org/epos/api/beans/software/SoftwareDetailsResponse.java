package org.epos.api.beans.software;

public abstract class SoftwareDetailsResponse {
	private final String type;

	protected SoftwareDetailsResponse(String type) {
		this.type = type;
	}

	public String getType() {
		return type;
	}
}
