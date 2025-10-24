package org.epos.api.beans.software;

import org.epos.api.core.software.SoftwareSourceCodeGenerationJPA;
import org.epos.eposdatamodel.SoftwareSourceCode;

public class SoftwareSourceCodeDetails extends SoftwareDetailsResponse {
	private final SoftwareSourceCodeResponse object;

	public SoftwareSourceCodeDetails(SoftwareSourceCode softwareSourceCode) {
		super("software_source_code");
		this.object = SoftwareSourceCodeGenerationJPA.generate(softwareSourceCode);
	}

	public SoftwareSourceCodeResponse getObject() {
		return object;
	}
}
