package org.epos.api.beans.software;

import org.epos.eposdatamodel.SoftwareSourceCode;

public class SoftwareSourceCodeDetails extends SoftwareDetailsResponse {
	private final SoftwareSourceCodeResponse softwareSourceCode;

	public SoftwareSourceCodeDetails(SoftwareSourceCode softwareSourceCode) {
		super("software_source_code");
		this.softwareSourceCode = new SoftwareSourceCodeResponse(softwareSourceCode);
	}

	public SoftwareSourceCodeResponse getSoftwareSourceCode() {
		return softwareSourceCode;
	}
}
