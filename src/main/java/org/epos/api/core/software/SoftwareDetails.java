package org.epos.api.core.software;

import org.epos.eposdatamodel.SoftwareApplication;
import org.epos.eposdatamodel.SoftwareSourceCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import abstractapis.AbstractAPI;
import metadataapis.EntityNames;

public class SoftwareDetails {
	private static final Logger LOGGER = LoggerFactory.getLogger(SoftwareDetails.class);

	public static Object generate(String instanceID) {
		LOGGER.info("Generating details for software with instanceID: {}", instanceID);

		var distribution = (org.epos.eposdatamodel.Distribution) AbstractAPI
				.retrieveAPI(EntityNames.DISTRIBUTION.name())
				.retrieve(instanceID);
		if (distribution != null) {
			return distribution;
		}

		SoftwareSourceCode softwareSourceCode = (SoftwareSourceCode) AbstractAPI
				.retrieveAPI(EntityNames.SOFTWARESOURCECODE.name()).retrieve(instanceID);
		if (softwareSourceCode != null) {
			return softwareSourceCode;
		}

		SoftwareApplication softwareApplication = (SoftwareApplication) AbstractAPI
				.retrieveAPI(EntityNames.SOFTWAREAPPLICATION.name()).retrieve(instanceID);
		if (softwareApplication != null) {
			return softwareApplication;
		}

		return null;
	}
}
