package org.epos.api.core;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import abstractapis.AbstractAPI;
import commonapis.LinkedEntityAPI;
import metadataapis.EntityNames;
import org.epos.api.beans.AvailableFormat;
import org.epos.api.beans.AvailableFormatConverted;
import org.epos.api.beans.Plugin;
import org.epos.api.enums.AvailableFormatType;
import org.epos.api.routines.DatabaseConnections;
import org.epos.eposdatamodel.Distribution;
import org.epos.eposdatamodel.LinkedEntity;
import org.epos.eposdatamodel.Mapping;
import org.epos.eposdatamodel.Operation;

/**
 * Optimized version of AvailableFormatsGeneration with:
 * - Batch fetching of mappings (eliminates N+1 queries)
 * - Parallel processing for large lists
 * - Reduced object allocations
 * - Cleaner code structure
 */
public class AvailableFormatsGeneration {

    private static final String API_PATH_EXECUTE = EnvironmentVariables.API_CONTEXT + "/execute/";
    private static final String API_PATH_EXECUTE_OGC = EnvironmentVariables.API_CONTEXT + "/ogcexecute/";
    private static final String API_FORMAT = "?format=";
    private static final String API_INPUT_FORMAT = "inputFormat=";
    private static final String API_PLUGIN_ID = "pluginId=";

    private static Map<String, List<AvailableFormat>> formats;

    public static Map<String, List<AvailableFormat>> getFormats() {
        return formats;
    }

    /**
     * Helper method to create AvailableFormat objects
     */
    private static AvailableFormat buildAvailableFormat(String originalFormat, String format, String href,
                                                        String label, AvailableFormatType type) {
        return new AvailableFormat.AvailableFormatBuilder()
                .originalFormat(originalFormat)
                .format(format)
                .href(href)
                .label(label)
                .type(type)
                .build();
    }

    /**
     * Helper method to create AvailableFormatConverted objects
     */
    private static AvailableFormat buildAvailableFormatConverted(String inputFormat, String pluginId,
                                                                 String originalFormat, String format, String href,
                                                                 String label, AvailableFormatType type) {
        return new AvailableFormatConverted.AvailableFormatConvertedBuilder()
                .inputFormat(inputFormat)
                .pluginId(pluginId)
                .originalFormat(originalFormat)
                .format(format)
                .href(href)
                .label(label)
                .type(type)
                .build();
    }

    /**
     * Generate formats for a list of distributions (with pre-computed map)
     */
    public static Map<String, List<AvailableFormat>> generate(List<Distribution> distributions) {
        formats = new ConcurrentHashMap<>();
        distributions.forEach(distribution -> {
            formats.put(distribution.getInstanceId(), generate(distribution));
        });
        return formats;
    }

    /**
     * Generate formats for all distributions (optimized with parallel streams)
     */
    public static Map<String, List<AvailableFormat>> generate() {
        List<Distribution> distributions = (List<Distribution>) AbstractAPI
                .retrieveAPI(EntityNames.DISTRIBUTION.name())
                .retrieveAll();

        formats = new ConcurrentHashMap<>();

        // Use parallel processing for large lists
        if (distributions.size() > 100) {
            distributions.parallelStream().forEach(distribution -> {
                if (distribution != null) {
                    formats.put(distribution.getInstanceId(), generate(distribution));
                }
            });
        } else {
            distributions.forEach(distribution -> {
                if (distribution != null) {
                    formats.put(distribution.getInstanceId(), generate(distribution));
                }
            });
        }

        return formats;
    }

    /**
     * Generate formats for a single distribution (OPTIMIZED with batch fetching)
     */
    public static List<AvailableFormat> generate(Distribution distribution) {
        List<AvailableFormat> formats = new ArrayList<>();

        // DOWNLOADABLE FILE
        if (distribution.getDownloadURL() != null && distribution.getAccessService() == null
                && !distribution.getDownloadURL().isEmpty() && distribution.getFormat() != null) {
            String[] uri = distribution.getFormat().split("/");
            String format = uri[uri.length - 1];
            formats.add(buildAvailableFormat(format, format, String.join(",", distribution.getDownloadURL()),
                    format.toUpperCase(), AvailableFormatType.ORIGINAL));
            return formats;
        }

        // If the operation for this distribution is null, return empty formats
        if (distribution.getSupportedOperation() == null || distribution.getSupportedOperation().isEmpty()) {
            return formats;
        }

        // WEBSERVICE - retrieve operation
        Operation operation = (Operation) LinkedEntityAPI.retrieveFromLinkedEntity(
                distribution.getSupportedOperation().get(0));

        if (operation == null) {
            return formats;
        }

        boolean isOgcFormat = false;

        // Process plugins if available
        if (DatabaseConnections.getInstance().getPlugins().containsKey(distribution.getInstanceId())) {
            for (Plugin.Relations relation : DatabaseConnections.getInstance().getPlugins()
                    .get(distribution.getInstanceId())) {
                processPluginRelation(relation, distribution, formats);
            }
        }

        // CRITICAL OPTIMIZATION: Batch fetch all mappings at once instead of one by one
        if (operation.getMapping() != null && !operation.getMapping().isEmpty() && operation.getTemplate() != null) {
            List<String> mappingIds = operation.getMapping().stream()
                    .map(LinkedEntity::getInstanceId)
                    .collect(Collectors.toList());

            // Single batch fetch instead of N individual queries
            List<Mapping> mappings = (List<Mapping>) AbstractAPI
                    .retrieveAPI(EntityNames.MAPPING.name())
                    .retrieveBunch(mappingIds);

            if (mappings != null && !mappings.isEmpty()) {
                isOgcFormat = processMappings(mappings, operation, distribution, formats);
            }
        }

        // Process returns if no formats found yet
        if (operation.getReturns() != null && formats.isEmpty()) {
            for (String returns : operation.getReturns()) {
                if (returns.contains("geojson") || returns.contains("geo+json")) {
                    formats.add(buildAvailableFormat(
                            returns,
                            "application/epos.geo+json",
                            buildHref(distribution, returns),
                            "GEOJSON",
                            AvailableFormatType.ORIGINAL));
                } else {
                    formats.add(buildAvailableFormat(
                            returns,
                            returns,
                            buildHref(distribution, returns),
                            returns.toUpperCase(),
                            AvailableFormatType.ORIGINAL));
                }
            }
        }

        return formats;
    }

    /**
     * Process plugin relations and add converted formats
     */
    private static void processPluginRelation(Plugin.Relations relation, Distribution distribution,
                                              List<AvailableFormat> formats) {
        String outputFormat = relation.getOutputFormat();

        if (outputFormat.equals("application/epos.geo+json")
                || outputFormat.equals("application/epos.table.geo+json")
                || outputFormat.equals("application/epos.map.geo+json")) {
            formats.add(buildAvailableFormatConverted(
                    relation.getInputFormat(),
                    relation.getPluginId(),
                    relation.getInputFormat(),
                    outputFormat,
                    buildHrefConverted(distribution, outputFormat, relation.getInputFormat(), relation.getPluginId()),
                    "GEOJSON",
                    AvailableFormatType.CONVERTED));
        } else if (outputFormat.equals("application/epos.graph.covjson")
                || outputFormat.equals("application/epos.covjson")) {
            formats.add(buildAvailableFormatConverted(
                    relation.getInputFormat(),
                    relation.getPluginId(),
                    relation.getInputFormat(),
                    outputFormat,
                    buildHrefConverted(distribution, outputFormat, relation.getInputFormat(), relation.getPluginId()),
                    "COVJSON",
                    AvailableFormatType.CONVERTED));
        }
    }

    /**
     * Process mappings to determine available formats (OPTIMIZED - uses pre-fetched mappings)
     *
     * @return true if OGC format was detected
     */
    private static boolean processMappings(List<Mapping> mappings, Operation operation,
                                           Distribution distribution, List<AvailableFormat> formats) {
        boolean isOgcFormat = false;

        for (Mapping map : mappings) {
            if (map == null || map.getProperty() == null || !map.getProperty().contains("encodingFormat")) {
                continue;
            }

            for (String pv : map.getParamValue()) {
                // OGC Format Check - Image formats
                if (pv.startsWith("image/")) {
                    if (operation.getTemplate().toLowerCase().contains("service=wms")
                            || containsServiceInMappings(mappings, "WMS", map)) {
                        formats.add(buildAvailableFormat(
                                pv,
                                "application/vnd.ogc.wms_xml",
                                buildHrefOgc(distribution),
                                "WMS",
                                AvailableFormatType.ORIGINAL));
                        isOgcFormat = true;
                    } else if (operation.getTemplate().toLowerCase().contains("service=wmts")
                            || containsServiceInMappings(mappings, "WMTS", map)) {
                        formats.add(buildAvailableFormat(
                                pv,
                                "application/vnd.ogc.wmts_xml",
                                buildHrefOgc(distribution),
                                "WMTS",
                                AvailableFormatType.ORIGINAL));
                        isOgcFormat = true;
                    }
                }
                // WFS with JSON
                else if (pv.equals("json") && operation.getTemplate() != null
                        && (operation.getTemplate().toLowerCase().contains("service=wfs")
                        || containsServiceInMappings(mappings, "WFS", map))) {
                    formats.add(buildAvailableFormat(
                            pv,
                            "application/epos.geo+json",
                            buildHref(distribution, "json"),
                            "GEOJSON (" + pv + ")",
                            AvailableFormatType.ORIGINAL));
                }
                // GeoJSON variants
                else if (pv.contains("geo%2Bjson") || pv.toLowerCase().matches(".*geo(?:json|\\+json|-json).*")) {
                    formats.add(buildAvailableFormat(
                            pv,
                            "application/epos.geo+json",
                            buildHref(distribution, pv),
                            "GEOJSON (" + pv + ")",
                            AvailableFormatType.ORIGINAL));
                }
                // Other formats
                else {
                    formats.add(buildAvailableFormat(
                            pv,
                            pv,
                            buildHref(distribution, pv),
                            pv.toUpperCase(),
                            AvailableFormatType.ORIGINAL));
                }
            }
        }

        return isOgcFormat;
    }

    /**
     * Check if a service exists in the mappings
     */
    private static boolean containsServiceInMappings(List<Mapping> mappings, String service, Mapping currentMap) {
        return mappings.stream()
                .filter(Objects::nonNull)
                .anyMatch(e -> e.getVariable() != null
                        && e.getVariable().equalsIgnoreCase("service")
                        && ((currentMap.getParamValue() != null && currentMap.getParamValue().contains(service))
                        || (e.getDefaultValue() != null && e.getDefaultValue().toLowerCase().contains(service.toLowerCase()))));
    }

    /**
     * Build href for regular execution
     */
    private static String buildHref(Distribution distribution, String format) {
        return EnvironmentVariables.API_HOST + API_PATH_EXECUTE + distribution.getInstanceId() + API_FORMAT + format;
    }

    /**
     * Build href for converted format execution
     */
    private static String buildHrefConverted(Distribution distribution, String outputFormat,
                                             String inputFormat, String pluginId) {
        return buildHref(distribution, outputFormat) + "&" + API_INPUT_FORMAT + inputFormat + "&" + API_PLUGIN_ID + pluginId;
    }

    /**
     * Build href for OGC execution
     */
    private static String buildHrefOgc(Distribution distribution) {
        return EnvironmentVariables.API_HOST + API_PATH_EXECUTE_OGC + distribution.getInstanceId();
    }
}