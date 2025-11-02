package org.epos.api.core;

import org.epos.eposdatamodel.Location;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PreFetchedEntities {

    public Map<String, Object> addresses = new HashMap<>();
    public Map<String, Object> identifiers = new HashMap<>();
    public Map<String, Object> locations = new HashMap<>();
    public Map<String, Object> temporals = new HashMap<>();
    public Map<String, Object> organizations = new HashMap<>();
    public Map<String, Object> categories = new HashMap<>();
    public Map<String, Object> contactPoints = new HashMap<>();
    public Map<String, Object> persons = new HashMap<>();
    public Map<String, Object> documentations = new HashMap<>();
    public Map<String, Object> operations = new HashMap<>();
    public Map<String, Object> mappings = new HashMap<>();
    public Map<String, Object> distributions = new ConcurrentHashMap<>();
    public Map<String, Object> webServices = new ConcurrentHashMap<>();
}
