package org.epos.api.beans.software;

import org.epos.api.beans.Distribution;

public class DistributionDetails extends SoftwareDetailsResponse {
	private final Distribution object;

	public DistributionDetails(org.epos.eposdatamodel.Distribution distribution) {
		super("distribution");
		this.object = org.epos.api.core.distributions.DistributionDetailsGenerationJPA
				.generate(java.util.Map.of("id", distribution.getInstanceId()));
	}

	public Distribution getObject() {
		return object;
	}
}
