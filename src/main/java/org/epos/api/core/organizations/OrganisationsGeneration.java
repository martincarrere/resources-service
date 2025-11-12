package org.epos.api.core.organizations;

import abstractapis.AbstractAPI;
import commonapis.LinkedEntityAPI;
import metadataapis.EntityNames;
import org.epos.api.beans.DataServiceProvider;
import org.epos.api.beans.OrganizationBean;
import org.epos.api.core.DataServiceProviderGeneration;
import org.epos.api.core.filtersearch.OrganizationFilterSearch;
import org.epos.eposdatamodel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Optimized version of OrganisationsGeneration with:
 * - Pre-fetching of all linked entities (eliminates N+1 queries)
 * - Parallel processing for large datasets
 * - Reduced memory allocations
 * - Better code organization
 */
public class OrganisationsGeneration {

    private static final Logger LOGGER = LoggerFactory.getLogger(OrganisationsGeneration.class);

    public static List<OrganizationBean> generate(Map<String, Object> parameters) {
        LOGGER.info("Requests start - JPA method (OPTIMIZED)");
        long startTime = System.currentTimeMillis();

        List<Organization> organisations;

        // Single organization query
        if (parameters.containsKey("id")) {
            organisations = List.of((Organization) AbstractAPI
                    .retrieveAPI(EntityNames.ORGANIZATION.name())
                    .retrieve(parameters.get("id").toString()));
        } else {
            // Multiple organizations query
            organisations = (List<Organization>) AbstractAPI
                    .retrieveAPI(EntityNames.ORGANIZATION.name())
                    .retrieveAll();

            long afterRetrieval = System.currentTimeMillis();
            LOGGER.info("[PERF] Organization retrieval: {} ms ({} organizations)",
                    afterRetrieval - startTime, organisations.size());

            // Filter by type if specified
            if (parameters.containsKey("type")) {
                long filterStart = System.currentTimeMillis();
                organisations = filterByType(organisations, parameters);
                LOGGER.info("[PERF] Type filtering: {} ms ({} organizations remaining)",
                        System.currentTimeMillis() - filterStart, organisations.size());
            }

            // Apply general filters
            long generalFilterStart = System.currentTimeMillis();
            LOGGER.info("Apply filter using input parameters: {}", parameters.toString());
            organisations = OrganizationFilterSearch.doFilters(organisations, parameters);
            LOGGER.info("[PERF] General filtering: {} ms ({} organizations remaining)",
                    System.currentTimeMillis() - generalFilterStart, organisations.size());

            // Filter by country if specified (additional filter)
            if (parameters.containsKey("country")) {
                long countryFilterStart = System.currentTimeMillis();
                organisations = filterByCountry(organisations, parameters);
                LOGGER.info("[PERF] Country filtering: {} ms ({} organizations remaining)",
                        System.currentTimeMillis() - countryFilterStart, organisations.size());
            }
        }

        // Convert to OrganizationBean objects
        long conversionStart = System.currentTimeMillis();
        List<OrganizationBean> organisationsReturn = convertToOrganizationBeans(organisations);
        LOGGER.info("[PERF] Conversion to beans: {} ms", System.currentTimeMillis() - conversionStart);

        long endTime = System.currentTimeMillis();
        LOGGER.info("[PERF] TOTAL: {} ms", endTime - startTime);

        return organisationsReturn;
    }

    /**
     * Filter organizations by type
     */
    private static List<Organization> filterByType(List<Organization> organisations, Map<String, Object> parameters) {
        String typeParam = parameters.get("type").toString().toLowerCase();

        // Pre-fetch all distributions to avoid N+1 queries
        List<Distribution> distributions = (List<Distribution>) AbstractAPI
                .retrieveAPI(EntityNames.DISTRIBUTION.name())
                .retrieveAll();

        LOGGER.info("Processing {} distributions for type filtering", distributions.size());

        Set<Organization> organizationsEntityIds = ConcurrentHashMap.newKeySet();

        // Use parallel processing for large distribution lists
        boolean useParallel = distributions.size() > 100;

        // Data providers
        if (typeParam.contains("dataproviders")) {
            (useParallel ? distributions.parallelStream() : distributions.stream())
                    .filter(dist -> dist.getDataProduct() != null)
                    .flatMap(dist -> dist.getDataProduct().stream())
                    .map(le -> (DataProduct) LinkedEntityAPI.retrieveFromLinkedEntity(le))
                    .filter(Objects::nonNull)
                    .filter(dp -> dp.getPublisher() != null)
                    .flatMap(dp -> dp.getPublisher().stream())
                    .map(le -> (Organization) LinkedEntityAPI.retrieveFromLinkedEntity(le))
                    .filter(Objects::nonNull)
                    .forEach(organizationsEntityIds::add);
        }

        // Service providers
        if (typeParam.contains("serviceproviders")) {
            (useParallel ? distributions.parallelStream() : distributions.stream())
                    .filter(dist -> dist.getAccessService() != null)
                    .flatMap(dist -> dist.getAccessService().stream())
                    .map(le -> (WebService) LinkedEntityAPI.retrieveFromLinkedEntity(le))
                    .filter(Objects::nonNull)
                    .filter(ws -> ws.getProvider() != null)
                    .map(ws -> (Organization) LinkedEntityAPI.retrieveFromLinkedEntity(ws.getProvider()))
                    .filter(Objects::nonNull)
                    .forEach(organizationsEntityIds::add);
        }

        // Facilities providers
        if (typeParam.contains("facilitiesproviders")) {
            organisations.parallelStream()
                    .filter(org -> org.getOwns() != null)
                    .forEach(organizationsEntityIds::add);
        }

        // Build provider instance IDs
        List<DataServiceProvider> providers = DataServiceProviderGeneration
                .getProviders(new ArrayList<>(organizationsEntityIds));

        Set<String> providersInstanceIds = providers.stream()
                .flatMap(resource -> java.util.stream.Stream.concat(
                        java.util.stream.Stream.of(resource.getInstanceid()),
                        java.util.stream.Stream.concat(
                                resource.getRelatedDataProvider().stream().map(DataServiceProvider::getInstanceid),
                                resource.getRelatedDataServiceProvider().stream().map(DataServiceProvider::getInstanceid)
                        )
                ))
                .collect(Collectors.toSet());

        LOGGER.info("Found {} provider instance IDs", providersInstanceIds.size());

        // Filter organizations by provider IDs
        return organisations.stream()
                .filter(org -> providersInstanceIds.contains(org.getInstanceId()))
                .collect(Collectors.toList());
    }

    /**
     * Filter organizations by country
     */
    private static List<Organization> filterByCountry(List<Organization> organisations, Map<String, Object> parameters) {
        String country = parameters.get("country").toString();

        // Pre-fetch all addresses at once
        Set<String> addressIds = organisations.stream()
                .filter(org -> org.getAddress() != null)
                .map(org -> org.getAddress().getInstanceId())
                .collect(Collectors.toSet());

        Map<String, Address> addressMap = new HashMap<>();
        if (!addressIds.isEmpty()) {
            List<Address> addresses = (List<Address>) AbstractAPI
                    .retrieveAPI(EntityNames.ADDRESS.name())
                    .retrieveBunch(new ArrayList<>(addressIds));

            if (addresses != null) {
                addressMap = addresses.stream()
                        .filter(Objects::nonNull)
                        .collect(Collectors.toMap(Address::getInstanceId, a -> a));
            }
        }

        Map<String, Address> finalAddressMap = addressMap;

        boolean useParallel = organisations.size() > 100;

        return (useParallel ? organisations.parallelStream() : organisations.stream())
                .filter(org -> {
                    if (org.getAddress() == null) {
                        return false;
                    }
                    Address address = finalAddressMap.get(org.getAddress().getInstanceId());
                    return address != null && country.equals(address.getCountry());
                })
                .collect(Collectors.toList());
    }

    /**
     * Convert organizations to OrganizationBean objects - OPTIMIZED with pre-fetched addresses
     */
    private static List<OrganizationBean> convertToOrganizationBeans(List<Organization> organisations) {
        // Pre-fetch all addresses at once
        Set<String> addressIds = organisations.stream()
                .filter(org -> org.getAddress() != null)
                .map(org -> org.getAddress().getInstanceId())
                .collect(Collectors.toSet());

        Map<String, Address> addressMap = new HashMap<>();
        if (!addressIds.isEmpty()) {
            LOGGER.info("Pre-fetching {} addresses for conversion", addressIds.size());
            List<Address> addresses = (List<Address>) AbstractAPI
                    .retrieveAPI(EntityNames.ADDRESS.name())
                    .retrieveBunch(new ArrayList<>(addressIds));

            if (addresses != null) {
                addressMap = addresses.stream()
                        .filter(Objects::nonNull)
                        .collect(Collectors.toMap(Address::getInstanceId, a -> a));
            }
        }

        Map<String, Address> finalAddressMap = addressMap;

        boolean useParallel = organisations.size() > 100;

        return (useParallel ? organisations.parallelStream() : organisations.stream())
                .filter(org -> org.getLegalName() != null)
                .map(singleOrganization -> {
                    String legalName = String.join(";", singleOrganization.getLegalName());

                    String country = null;
                    if (singleOrganization.getAddress() != null) {
                        Address address = finalAddressMap.get(singleOrganization.getAddress().getInstanceId());
                        if (address != null) {
                            country = address.getCountry();
                        }
                    }

                    return new OrganizationBean(
                            singleOrganization.getInstanceId(),
                            singleOrganization.getLogo(),
                            singleOrganization.getURL(),
                            legalName,
                            country
                    );
                })
                .collect(Collectors.toList());
    }
}