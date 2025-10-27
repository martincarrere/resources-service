package org.epos.api.core.software;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epos.api.beans.AvailableContactPoints;
import org.epos.api.beans.DiscoveryItem;
import org.epos.api.beans.software.SoftwareSourceCodeResponse;
import org.epos.api.core.EnvironmentVariables;
import org.epos.api.enums.ProviderType;
import org.epos.api.facets.Facets;
import org.epos.api.facets.FacetsGeneration;
import org.epos.eposdatamodel.Category;
import org.epos.eposdatamodel.Identifier;
import org.epos.eposdatamodel.SoftwareSourceCode;

import commonapis.LinkedEntityAPI;

public class SoftwareSourceCodeGenerationJPA {

	private static final Logger logger = LogManager.getLogger(SoftwareSourceCodeGenerationJPA.class);
	private static final String EMAIL_SENDER = EnvironmentVariables.API_CONTEXT + "/sender/send-email?id=";

	public static SoftwareSourceCodeResponse generate(SoftwareSourceCode softwareSourceCode) {
		SoftwareSourceCodeResponse response = new SoftwareSourceCodeResponse();

		response.setName(softwareSourceCode.getName());
		response.setDescription(softwareSourceCode.getDescription());
		response.setCodeRepository(softwareSourceCode.getCodeRepository());
		// download url is not part of the EPOS-DCAT-AP model for SoftwareSourceCode, why is it here?
		// response.setDownloadURL(softwareSourceCode.getDownloadURL());
		response.setLicenseURL(softwareSourceCode.getLicenseURL());
		response.setMainEntityOfPage(softwareSourceCode.getMainEntityofPage());
		response.setProgrammingLanguage(softwareSourceCode.getProgrammingLanguage());
		response.setRuntimePlatform(softwareSourceCode.getRuntimePlatform());
		response.setSoftwareVersion(softwareSourceCode.getSoftwareVersion());
		response.setSoftwareStatus(softwareSourceCode.getSoftwareStatus());
		response.setSpatial(softwareSourceCode.getSpatial());
		response.setTemporal(softwareSourceCode.getTemporal());
		String keywordsStr = softwareSourceCode.getKeywords();
		if (keywordsStr != null && !keywordsStr.trim().isEmpty()) {
			List<String> kw = List.of(keywordsStr.split(",")).stream()
					.map(String::trim)
					.filter(s -> !s.isEmpty())
					.toList();
			response.setKeywords(kw.isEmpty() ? null : kw);
		} else {
			response.setKeywords(null);
		}
		response.setCitation(softwareSourceCode.getCitation());
		response.setSize(softwareSourceCode.getSize());
		response.setTimeRequired(softwareSourceCode.getTimeRequired());
		response.setId(softwareSourceCode.getInstanceId());

		// Identifiers
		logger.info("SoftwareSourceCodeGenerationJPA.generate: softwareSourceCode.getIdentifier()={}",
				softwareSourceCode.getIdentifier());
		List<String> doi = new ArrayList<>();
		List<String> identifiers = new ArrayList<>();
		if (softwareSourceCode.getIdentifier() != null) {
			softwareSourceCode.getIdentifier().forEach(identifierLe -> {
				Identifier identifier = (Identifier) LinkedEntityAPI.retrieveFromLinkedEntity(identifierLe);
				if (identifier != null) {
					if (identifier.getType().equals("DOI")) {
						doi.add(identifier.getIdentifier());
					}
					if (identifier.getType().equals("DDSS-ID")) {
						String ddss = identifier.getIdentifier();
						if (ddss != null) {
							identifiers.add(ddss);
						}
					}
				}
			});
		}
		response.setDoi(doi.isEmpty() ? null : doi);
		response.setIdentifiers(identifiers.isEmpty() ? null : identifiers);

		// Contact Points
		logger.info("SoftwareSourceCodeGenerationJPA.generate: softwareSourceCode.getContactPoint()={}",
				softwareSourceCode.getContactPoint());
		if (softwareSourceCode.getContactPoint() != null && !softwareSourceCode.getContactPoint().isEmpty()) {
			response.getAvailableContactPoints()
					.add(new AvailableContactPoints.AvailableContactPointsBuilder()
							.href(EnvironmentVariables.API_HOST + EMAIL_SENDER + softwareSourceCode.getInstanceId()
									+ "&contactType=" + ProviderType.DATAPROVIDERS)
							.type(ProviderType.DATAPROVIDERS).build());
		}
		if (response.getAvailableContactPoints().isEmpty()) {
			response.setAvailableContactPoints(null);
		}

		// Categories
		logger.info("SoftwareSourceCodeGenerationJPA.generate: softwareSourceCode.getCategory()={}",
				softwareSourceCode.getCategory());
		if (softwareSourceCode.getCategory() != null) {
			List<String> categoryList = softwareSourceCode.getCategory().stream()
					.map(linkedEntity -> (Category) LinkedEntityAPI.retrieveFromLinkedEntity(linkedEntity))
					.filter(Objects::nonNull)
					.map(Category::getUid)
					.filter(uid -> uid.contains("category:"))
					.collect(Collectors.toList());

			if (!categoryList.isEmpty()) {
				List<DiscoveryItem> discoveryList = new ArrayList<>();
				discoveryList.add(new DiscoveryItem.DiscoveryItemBuilder(softwareSourceCode.getInstanceId(), null, null)
						.categories(categoryList)
						.build());

				org.epos.api.facets.FacetsNodeTree categoriesTree = FacetsGeneration
						.generateResponseUsingCategories(discoveryList, Facets.Type.SOFTWARE);
				categoriesTree.getNodes().forEach(node -> node.setDistributions(null));
				response.setCategories(categoriesTree.getFacets());
			}
		}

		return response;
	}
}
