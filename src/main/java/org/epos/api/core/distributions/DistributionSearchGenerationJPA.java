package org.epos.api.core.distributions;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import abstractapis.AbstractAPI;
import commonapis.LinkedEntityAPI;
import metadataapis.EntityNames;
import org.apache.commons.codec.digest.DigestUtils;
import org.epos.api.beans.AvailableFormat;
import org.epos.api.beans.DataServiceProvider;
import org.epos.api.beans.DiscoveryItem;
import org.epos.api.beans.DiscoveryItem.DiscoveryItemBuilder;
import org.epos.api.beans.NodeFilters;
import org.epos.api.beans.SearchResponse;
import org.epos.api.core.AvailableFormatsGeneration;
import org.epos.api.core.DataServiceProviderGeneration;
import org.epos.api.core.EnvironmentVariables;
import org.epos.api.core.ZabbixExecutor;
import org.epos.api.core.filtersearch.DistributionFilterSearch;
import org.epos.api.facets.Facets;
import org.epos.api.facets.FacetsGeneration;
import org.epos.api.facets.Node;
import org.epos.api.routines.DatabaseConnections;
import org.epos.eposdatamodel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import model.StatusType;

/**
 * Optimized version of DistributionSearchGenerationJPA with:
 * - Pre-fetching of all linked entities (eliminates N+1 query problem)
 * - Parallel stream processing
 * - Thread-safe collections
 * - Performance logging
 * - Reduced memory allocations
 */
public class DistributionSearchGenerationJPA {

    private static final Logger LOGGER = LoggerFactory.getLogger(DistributionSearchGenerationJPA.class);
    private static final String API_PATH_DETAILS = EnvironmentVariables.API_CONTEXT + "/resources/details/";
    private static final String PARAMETER__SCIENCE_DOMAIN = "sciencedomains";
    private static final String PARAMETER__SERVICE_TYPE = "servicetypes";

    public static SearchResponse generate(Map<String, Object> parameters, User user) {
        LOGGER.info("Requests start - JPA method (OPTIMIZED)");
        long startTime = System.currentTimeMillis();

        // Determine access level and versions
        boolean isBackofficeUser = user != null;
        List<StatusType> versions = getVersions(parameters, isBackofficeUser);

        // Retrieve and filter dataproducts
        long retrievalStart = System.currentTimeMillis();
        Map<String, User> userMap = DatabaseConnections.retrieveUserMap();

        List<DataProduct> dataproducts = ((List<DataProduct>) AbstractAPI
                .retrieveAPI(EntityNames.DATAPRODUCT.name())
                .retrieveAll())
                .parallelStream()
                .filter(dp -> dp != null && shouldIncludeDataProduct(dp, versions, isBackofficeUser, user))
                .collect(Collectors.toList());

        LOGGER.info("[PERF] Data retrieval: {} ms ({} dataproducts)",
                System.currentTimeMillis() - retrievalStart, dataproducts.size());

        // Apply filters
        long filterStart = System.currentTimeMillis();
        LOGGER.info("Apply filter using input parameters: {}", parameters.toString());
        dataproducts = DistributionFilterSearch.doFilters(dataproducts, parameters);
        LOGGER.info("[PERF] Filtering: {} ms ({} dataproducts remaining)",
                System.currentTimeMillis() - filterStart, dataproducts.size());

        // Pre-fetch all linked entities (CRITICAL OPTIMIZATION)
        long prefetchStart = System.currentTimeMillis();
        PreFetchedEntities preFetched = preFetchLinkedEntities(dataproducts);
        LOGGER.info("[PERF] Pre-fetching entities: {} ms", System.currentTimeMillis() - prefetchStart);

        // Process dataproducts in parallel with thread-safe collections
        long processingStart = System.currentTimeMillis();
        Set<DiscoveryItem> discoveryMap = ConcurrentHashMap.newKeySet();
        Set<String> keywords = ConcurrentHashMap.newKeySet();
        Set<Category> scienceDomains = ConcurrentHashMap.newKeySet();
        Set<Category> serviceTypes = ConcurrentHashMap.newKeySet();
        Set<Organization> organizationsEntityIds = ConcurrentHashMap.newKeySet();

        LOGGER.info("Start parallel processing of {} dataproducts", dataproducts.size());

        dataproducts.parallelStream().forEach(dataproduct -> {
            processDataProduct(dataproduct, preFetched, discoveryMap, keywords,
                    scienceDomains, serviceTypes, organizationsEntityIds,
                    parameters, isBackofficeUser, userMap);
        });

        LOGGER.info("[PERF] Processing: {} ms", System.currentTimeMillis() - processingStart);
        LOGGER.info("Final number of results: {}", discoveryMap.size());

        // Build response
        long responseStart = System.currentTimeMillis();
        SearchResponse response = buildResponse(discoveryMap, keywords, organizationsEntityIds,
                scienceDomains, serviceTypes, parameters);
        LOGGER.info("[PERF] Response building: {} ms", System.currentTimeMillis() - responseStart);

        long endTime = System.currentTimeMillis();
        long duration = (endTime - startTime);
        LOGGER.info("[PERF] TOTAL: {} ms", duration);

        return response;
    }

    /**
     * Determines which versions to include based on user permissions
     */
    private static List<StatusType> getVersions(Map<String, Object> parameters, boolean isBackofficeUser) {
        List<StatusType> versions = new ArrayList<>();
        if (isBackofficeUser && parameters.containsKey("versioningStatus")) {
            Arrays.stream(parameters.get("versioningStatus").toString().split(","))
                    .forEach(version -> versions.add(StatusType.valueOf(version)));
        } else {
            versions.add(StatusType.PUBLISHED);
        }
        return versions;
    }

    /**
     * Checks if a dataproduct should be included based on status and user permissions
     */
    private static boolean shouldIncludeDataProduct(DataProduct dp, List<StatusType> versions,
                                                    boolean isBackofficeUser, User user) {
        if (versions.contains(StatusType.PUBLISHED) && dp.getStatus().equals(StatusType.PUBLISHED)) {
            return true;
        }
        if (isBackofficeUser && user != null && versions.contains(dp.getStatus())) {
            return user.getIsAdmin() || dp.getEditorId().equals(user.getAuthIdentifier());
        }
        return false;
    }

    /**
     * Pre-fetches ALL linked entities to eliminate N+1 query problem
     * This is the most critical optimization - replaces thousands of individual queries with batch fetches
     */
    private static PreFetchedEntities preFetchLinkedEntities(List<DataProduct> dataproducts) {
        PreFetchedEntities entities = new PreFetchedEntities();

        // Collect all IDs that need to be fetched
        Set<String> categoryIds = ConcurrentHashMap.newKeySet();
        Set<String> organizationIds = ConcurrentHashMap.newKeySet();
        Set<String> distributionIds = ConcurrentHashMap.newKeySet();

        // Parallel collection of IDs
        dataproducts.parallelStream().forEach(dp -> {
            if (dp.getCategory() != null) {
                dp.getCategory().forEach(le -> categoryIds.add(le.getInstanceId()));
            }
            if (dp.getPublisher() != null) {
                dp.getPublisher().forEach(le -> organizationIds.add(le.getInstanceId()));
            }
            if (dp.getDistribution() != null) {
                dp.getDistribution().forEach(le -> distributionIds.add(le.getInstanceId()));
            }
        });

        LOGGER.info("Pre-fetching: {} categories, {} organizations, {} distributions",
                categoryIds.size(), organizationIds.size(), distributionIds.size());

        // Batch fetch all entities
        entities.categories = fetchBatch(EntityNames.CATEGORY, categoryIds);
        entities.organizations = fetchBatch(EntityNames.ORGANIZATION, organizationIds);
        entities.distributions = fetchBatch(EntityNames.DISTRIBUTION, distributionIds);

        // Collect web service IDs from distributions
        Set<String> webServiceIds = ConcurrentHashMap.newKeySet();
        entities.distributions.values().parallelStream()
                .filter(obj -> obj instanceof Distribution)
                .map(obj -> (Distribution) obj)
                .filter(dist -> dist.getAccessService() != null)
                .forEach(dist -> dist.getAccessService().forEach(le -> webServiceIds.add(le.getInstanceId())));

        LOGGER.info("Pre-fetching: {} web services", webServiceIds.size());
        entities.webServices = fetchBatch(EntityNames.WEBSERVICE, webServiceIds);

        // Collect organization IDs from web services
        Set<String> additionalOrgIds = ConcurrentHashMap.newKeySet();
        entities.webServices.values().parallelStream()
                .filter(obj -> obj instanceof WebService)
                .map(obj -> (WebService) obj)
                .filter(ws -> ws.getProvider() != null)
                .forEach(ws -> additionalOrgIds.add(ws.getProvider().getInstanceId()));

        // Fetch additional organizations
        if (!additionalOrgIds.isEmpty()) {
            LOGGER.info("Pre-fetching: {} additional organizations from web services", additionalOrgIds.size());
            Map<String, Object> additionalOrgs = fetchBatch(EntityNames.ORGANIZATION, additionalOrgIds);
            entities.organizations.putAll(additionalOrgs);
        }

        // Collect category IDs from web services
        Set<String> additionalCategoryIds = ConcurrentHashMap.newKeySet();
        entities.webServices.values().parallelStream()
                .filter(obj -> obj instanceof WebService)
                .map(obj -> (WebService) obj)
                .filter(ws -> ws.getCategory() != null)
                .forEach(ws -> ws.getCategory().forEach(le -> additionalCategoryIds.add(le.getInstanceId())));

        // Fetch additional categories
        if (!additionalCategoryIds.isEmpty()) {
            LOGGER.info("Pre-fetching: {} additional categories from web services", additionalCategoryIds.size());
            Map<String, Object> additionalCategories = fetchBatch(EntityNames.CATEGORY, additionalCategoryIds);
            entities.categories.putAll(additionalCategories);
        }

        return entities;
    }

    /**
     * Batch fetch helper that retrieves multiple entities in one call
     */
    private static Map<String, Object> fetchBatch(EntityNames entityName, Set<String> ids) {
        if (ids.isEmpty()) {
            return new ConcurrentHashMap<>();
        }

        List<String> idList = new ArrayList<>(ids);
        List<?> results = (List<?>) AbstractAPI.retrieveAPI(entityName.name()).retrieveBunch(idList);

        Map<String, Object> map = new ConcurrentHashMap<>();
        if (results != null) {
            results.parallelStream()
                    .filter(Objects::nonNull)
                    .filter(obj -> obj instanceof EPOSDataModelEntity)
                    .forEach(obj -> {
                        EPOSDataModelEntity entity = (EPOSDataModelEntity) obj;
                        map.put(entity.getInstanceId(), entity);
                    });
        }
        return map;
    }

    /**
     * Process a single dataproduct using pre-fetched entities
     */
    private static void processDataProduct(DataProduct dataproduct, PreFetchedEntities preFetched,
                                           Set<DiscoveryItem> discoveryMap, Set<String> keywords,
                                           Set<Category> scienceDomains, Set<Category> serviceTypes,
                                           Set<Organization> organizationsEntityIds,
                                           Map<String, Object> parameters, boolean isBackofficeUser,
                                           Map<String, User> userMap) {

        Set<String> facetsDataProviders = new HashSet<>();
        List<String> categoryList = new ArrayList<>();

        // Process categories - using pre-fetched data
        if (dataproduct.getCategory() != null) {
            dataproduct.getCategory().forEach(le -> {
                Category category = (Category) preFetched.categories.get(le.getInstanceId());
                if (category != null) {
                    if (category.getUid().contains("category:")) {
                        categoryList.add(category.getUid());
                    } else {
                        scienceDomains.add(category);
                    }
                }
            });
        }

        // Process publishers - using pre-fetched data
        if (dataproduct.getPublisher() != null) {
            dataproduct.getPublisher().forEach(le -> {
                Organization org = (Organization) preFetched.organizations.get(le.getInstanceId());
                if (org != null && org.getLegalName() != null) {
                    facetsDataProviders.add(String.join(",", org.getLegalName()));
                    organizationsEntityIds.add(org);
                }
            });
        }

        // Process keywords
        if (dataproduct.getKeywords() != null && !dataproduct.getKeywords().isEmpty()) {
            Arrays.stream(dataproduct.getKeywords().split(",\t"))
                    .map(String::toLowerCase)
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .forEach(keywords::add);
        }

        // Process distributions - using pre-fetched data
        if (dataproduct.getDistribution() != null) {
            dataproduct.getDistribution().forEach(le -> {
                Distribution distribution = (Distribution) preFetched.distributions.get(le.getInstanceId());
                if (distribution != null) {
                    processDistribution(distribution, preFetched, discoveryMap, serviceTypes,
                            organizationsEntityIds, facetsDataProviders, categoryList,
                            parameters, isBackofficeUser, userMap, dataproduct);
                }
            });
        }
    }

    /**
     * Process a single distribution using pre-fetched entities
     */
    private static void processDistribution(Distribution distribution, PreFetchedEntities preFetched,
                                            Set<DiscoveryItem> discoveryMap, Set<Category> serviceTypes,
                                            Set<Organization> organizationsEntityIds,
                                            Set<String> facetsDataProviders, List<String> categoryList,
                                            Map<String, Object> parameters, boolean isBackofficeUser,
                                            Map<String, User> userMap, DataProduct dataproduct) {

        Set<String> facetsServiceProviders = new HashSet<>();

        // Generate available formats
        List<AvailableFormat> availableFormats = AvailableFormatsGeneration.generate(distribution);
        List<DataServiceProvider> dataServiceProviderList = new ArrayList<>();

        // Process access services - using pre-fetched data
        if (distribution.getAccessService() != null) {
            distribution.getAccessService().forEach(le -> {
                WebService webService = (WebService) preFetched.webServices.get(le.getInstanceId());
                if (webService != null) {
                    // Process provider
                    if (webService.getProvider() != null) {
                        Organization org = (Organization) preFetched.organizations.get(
                                webService.getProvider().getInstanceId());
                        if (org != null) {
                            if (org.getLegalName() != null) {
                                facetsServiceProviders.add(String.join(",", org.getLegalName()));
                            }
                            organizationsEntityIds.add(org);

                            List<DataServiceProvider> providers = DataServiceProviderGeneration.getProviders(List.of(org));
                            if (!providers.isEmpty()) {
                                dataServiceProviderList.add(providers.get(0));
                            }
                        }
                    }

                    // Process service categories
                    if (webService.getCategory() != null) {
                        webService.getCategory().forEach(catLe -> {
                            Category category = (Category) preFetched.categories.get(catLe.getInstanceId());
                            if (category != null) {
                                serviceTypes.add(category);
                            }
                        });
                    }
                }
            });
        }

        // Build discovery item
        DiscoveryItemBuilder discoveryItemBuilder = new DiscoveryItemBuilder(
                distribution.getInstanceId(),
                EnvironmentVariables.API_HOST + API_PATH_DETAILS + distribution.getInstanceId(),
                EnvironmentVariables.API_HOST + API_PATH_DETAILS + distribution.getInstanceId() + "?extended=true")
                .uid(distribution.getUid())
                .metaId(distribution.getMetaId())
                .title(distribution.getTitle() != null ? String.join(";", distribution.getTitle()) : null)
                .description(distribution.getDescription() != null ? String.join(";", distribution.getDescription()) : null)
                .dataServiceProvider(dataServiceProviderList.isEmpty() ? null : dataServiceProviderList.get(0))
                .availableFormats(availableFormats)
                .sha256id(distribution.getUid() != null ? DigestUtils.sha256Hex(distribution.getUid()) : "")
                .dataProvider(facetsDataProviders)
                .serviceProvider(facetsServiceProviders)
                .categories(categoryList.isEmpty() ? null : categoryList);

        // Add backoffice info if needed
        if (isBackofficeUser && parameters.containsKey("versioningStatus")) {
            String editorId = distribution.getEditorId();
            if ("ingestor".equals(editorId)) {
                discoveryItemBuilder.editorFullName("Ingestor");
            } else {
                User editor = userMap.get(editorId);
                if (editor != null) {
                    discoveryItemBuilder.editorFullName(editor.getFirstName() + " " + editor.getLastName());
                }
            }
            discoveryItemBuilder.editorId(editorId)
                    .changeDate(distribution.getChangeTimestamp())
                    .versioningStatus(dataproduct.getStatus().name());
        }

        DiscoveryItem discoveryItem = discoveryItemBuilder.build();

        // Add monitoring info if enabled
        if (EnvironmentVariables.MONITORING != null && EnvironmentVariables.MONITORING.equals("true")) {
            discoveryItem.setStatus(ZabbixExecutor.getInstance().getStatusInfoFromSha(discoveryItem.getSha256id()));
            discoveryItem.setStatusTimestamp(
                    ZabbixExecutor.getInstance().getStatusTimestampInfoFromSha(discoveryItem.getSha256id()));
        }

        discoveryMap.add(discoveryItem);
    }

    /**
     * Build the final search response with results and filters
     */
    private static SearchResponse buildResponse(Set<DiscoveryItem> discoveryMap, Set<String> keywords,
                                                Set<Organization> organizationsEntityIds,
                                                Set<Category> scienceDomains, Set<Category> serviceTypes,
                                                Map<String, Object> parameters) {

        Node results = new Node("results");

        // Build facets or regular results
        if (parameters.containsKey("facets") && parameters.get("facets").toString().equals("true")) {
            String facetsType = parameters.get("facetstype").toString();
            switch (facetsType) {
                case "categories":
                    results.addChild(FacetsGeneration.generateResponseUsingCategories(discoveryMap, Facets.Type.DATA).getFacets());
                    break;
                case "dataproviders":
                    results.addChild(FacetsGeneration.generateResponseUsingDataproviders(discoveryMap).getFacets());
                    break;
                case "serviceproviders":
                    results.addChild(FacetsGeneration.generateResponseUsingServiceproviders(discoveryMap).getFacets());
                    break;
                default:
                    Node child = new Node();
                    child.setDistributions(discoveryMap);
                    results.addChild(child);
                    break;
            }
        } else {
            Node child = new Node();
            child.setDistributions(discoveryMap);
            results.addChild(child);
        }

        LOGGER.info("Number of organizations retrieved: {}", organizationsEntityIds.size());

        // Build keywords filter
        List<String> keywordsCollection = keywords.stream()
                .filter(Objects::nonNull)
                .sorted()
                .collect(Collectors.toList());

        NodeFilters keywordsNodes = new NodeFilters("keywords");
        keywordsCollection.forEach(keyword -> {
            NodeFilters node = new NodeFilters(keyword);
            node.setId(Base64.getEncoder().encodeToString(keyword.getBytes()));
            keywordsNodes.addChild(node);
        });

        // Build organizations filter
        List<DataServiceProvider> collection = DataServiceProviderGeneration
                .getProviders(new ArrayList<>(organizationsEntityIds));

        NodeFilters organisationsNodes = new NodeFilters("organisations");
        collection.forEach(resource -> {
            NodeFilters node = new NodeFilters(resource.getDataProviderLegalName());
            node.setId(resource.getInstanceid());
            organisationsNodes.addChild(node);

            resource.getRelatedDataProvider().forEach(relatedData -> {
                NodeFilters relatedNodeDataProvider = new NodeFilters(relatedData.getDataProviderLegalName());
                relatedNodeDataProvider.setId(relatedData.getInstanceid());
                organisationsNodes.addChild(relatedNodeDataProvider);
            });

            resource.getRelatedDataServiceProvider().forEach(relatedDataService -> {
                NodeFilters relatedNodeDataServiceProvider = new NodeFilters(
                        relatedDataService.getDataProviderLegalName());
                relatedNodeDataServiceProvider.setId(relatedDataService.getInstanceid());
                organisationsNodes.addChild(relatedNodeDataServiceProvider);
            });
        });

        // Build science domains filter
        NodeFilters scienceDomainsNodes = new NodeFilters(PARAMETER__SCIENCE_DOMAIN);
        scienceDomains.forEach(r -> scienceDomainsNodes.addChild(new NodeFilters(r.getInstanceId(), r.getName())));

        // Build service types filter
        NodeFilters serviceTypesNodes = new NodeFilters(PARAMETER__SERVICE_TYPE);
        serviceTypes.forEach(r -> serviceTypesNodes.addChild(new NodeFilters(r.getInstanceId(), r.getName())));

        // Combine all filters
        ArrayList<NodeFilters> filters = new ArrayList<>();
        filters.add(keywordsNodes);
        filters.add(organisationsNodes);
        filters.add(scienceDomainsNodes);
        filters.add(serviceTypesNodes);

        return new SearchResponse(results, filters);
    }

    /**
     * Container class for pre-fetched entities
     */
    private static class PreFetchedEntities {
        Map<String, Object> categories = new ConcurrentHashMap<>();
        Map<String, Object> organizations = new ConcurrentHashMap<>();
        Map<String, Object> distributions = new ConcurrentHashMap<>();
        Map<String, Object> webServices = new ConcurrentHashMap<>();
    }
}