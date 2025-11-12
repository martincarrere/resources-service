package org.epos.api.core.distributions;

import java.util.*;
import java.util.stream.Collectors;

import abstractapis.AbstractAPI;
import metadataapis.EntityNames;
import org.epos.api.beans.Parameter;
import org.epos.api.beans.ParametersResponse;
import org.epos.eposdatamodel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import commonapis.LinkedEntityAPI;

public class LinkedEntityParametersSearch {
    private static final Logger LOGGER = LoggerFactory.getLogger(LinkedEntityParametersSearch.class);

    public static ParametersResponse generate(String id) {
        long startTime = System.currentTimeMillis();
        LOGGER.info("Starting parameter search (OPTIMIZED) for distribution id: {}", id);

        // Find the distribution
        long retrievalStart = System.currentTimeMillis();
        Distribution distribution = (Distribution) AbstractAPI
                .retrieveAPI(EntityNames.DISTRIBUTION.name())
                .retrieve(id);

        if (distribution == null || distribution.getSupportedOperation() == null
                || distribution.getSupportedOperation().isEmpty()) {
            LOGGER.warn("Distribution {} not found or has no supported operations.", id);
            return null;
        }
        LOGGER.info("[PERF] Distribution retrieval: {} ms", System.currentTimeMillis() - retrievalStart);
        LOGGER.debug("Found distribution {} with supported operations.", id);

        // Get operation (assuming only one operation for a distribution)
        long operationStart = System.currentTimeMillis();
        Operation operation = null;
        try {
            operation = (Operation) LinkedEntityAPI
                    .retrieveFromLinkedEntity(distribution.getSupportedOperation().get(0));
        } catch (Exception e) {
            LOGGER.error("Error while retrieving operation from linked entity for distribution {}", id, e);
            return null;
        }

        if (operation == null || operation.getPayload() == null || operation.getPayload().isEmpty()) {
            LOGGER.warn("Operation not found or has no payload for distribution {}.", id);
            return null;
        }
        LOGGER.info("[PERF] Operation retrieval: {} ms", System.currentTimeMillis() - operationStart);
        LOGGER.debug("Using operation id {} for distribution {}.", operation.getInstanceId(), id);

        // Get payload (assuming only one payload for an operation)
        long payloadStart = System.currentTimeMillis();
        String payloadId = operation.getPayload().get(0).getInstanceId();
        LOGGER.debug("Using payload id {} for operation {}.", payloadId, operation.getInstanceId());

        Payload payload = (Payload) AbstractAPI.retrieveAPI(EntityNames.PAYLOAD.name()).retrieve(payloadId);

        if (payload == null || payload.getOutputMapping() == null || payload.getOutputMapping().isEmpty()) {
            LOGGER.warn("Payload {} not found or has no output mapping for operation {}.",
                    payloadId, operation.getInstanceId());
            return null;
        }
        LOGGER.info("[PERF] Payload retrieval: {} ms", System.currentTimeMillis() - payloadStart);
        LOGGER.debug("Payload {} found with {} output mappings.", payloadId, payload.getOutputMapping().size());

        // Batch fetch only relevant mappings instead of retrieveAll()
        long mappingStart = System.currentTimeMillis();
        Set<String> relevantMappingIds = payload.getOutputMapping().stream()
                .map(LinkedEntity::getInstanceId)
                .collect(Collectors.toSet());
        LOGGER.debug("Relevant mapping ids ({}): {}", relevantMappingIds.size(), relevantMappingIds);

        // retrieveBunch() fetches only needed mappings (FAST)
        Set<Parameter> parameters = new HashSet<>();
        if (!relevantMappingIds.isEmpty()) {
            List<OutputMapping> outputMappings = (List<OutputMapping>) AbstractAPI
                    .retrieveAPI(EntityNames.OUTPUTMAPPING.name())
                    .retrieveBunch(new ArrayList<>(relevantMappingIds));

            if (outputMappings != null) {
                outputMappings.stream()
                        .filter(Objects::nonNull)
                        .forEach(mapping -> {
                            parameters.add(new Parameter(mapping.getOutputProperty(), mapping.getOutputVariable()));
                            LOGGER.debug("Added parameter: {} -> {}",
                                    mapping.getOutputProperty(), mapping.getOutputVariable());
                        });
            }
        }

        LOGGER.info("[PERF] Output mapping retrieval and processing: {} ms",
                System.currentTimeMillis() - mappingStart);

        long endTime = System.currentTimeMillis();
        LOGGER.info("[PERF] TOTAL: {} ms - Successfully found {} parameters for distribution id {}.",
                endTime - startTime, parameters.size(), id);

        return new ParametersResponse(parameters);
    }
}