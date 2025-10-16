package org.epos.api.beans.software;

import org.epos.eposdatamodel.SoftwareApplication;

public class SoftwareApplicationDetails extends SoftwareDetailsResponse {
	private final SoftwareApplicationResponse softwareApplication;

	public SoftwareApplicationDetails(SoftwareApplication softwareApplication) {
		super("software_application");
		this.softwareApplication = new SoftwareApplicationResponse(softwareApplication);
	}

	public SoftwareApplicationResponse getSoftwareApplication() {
		return softwareApplication;
	}
}
