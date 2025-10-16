package org.epos.api.beans.software;

import org.epos.eposdatamodel.SoftwareApplication;

public class SoftwareApplicationDetails extends SoftwareDetailsResponse {
	private final SoftwareApplicationResponse object;

	public SoftwareApplicationDetails(SoftwareApplication softwareApplication) {
		super("software_application");
		this.object = new SoftwareApplicationResponse(softwareApplication);
	}

	public SoftwareApplicationResponse getObject() {
		return object;
	}
}
