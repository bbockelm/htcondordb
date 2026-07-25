package opensearchsync

// This file builds the index mapping the documents are indexed under, ported from
// condor_adstash's make_mappings/make_settings: explicit field types from the same attribute
// category tables the transform uses, plus dynamic templates that type unknown-but-patterned
// fields (matching the transform's auto-rules). date/numeric auto-detection is disabled.

// indexProperties returns the explicit per-attribute field mappings (the "properties" block).
// Split out from buildMappings so the additive mapping-patch (EnsureIndex) can diff the desired
// fields against an existing index and PUT only the missing ones.
func indexProperties() map[string]any {
	properties := map[string]any{
		// metadata is a nested object; its two runtime fields are typed as epoch-second dates.
		"metadata": map[string]any{
			"type":    "nested",
			"dynamic": true,
			"properties": map[string]any{
				"adstash_runtime":        epochDate(),
				"condor_history_runtime": epochDate(),
				"opensearchsync_runtime": epochDate(),
			},
		},
	}
	for _, a := range indexedKeywordAttrs {
		properties[a] = map[string]any{"type": "keyword"}
	}
	for _, a := range noindexKeywordAttrs {
		properties[a] = map[string]any{"type": "keyword", "index": false}
	}
	for _, a := range floatAttrs {
		properties[a] = map[string]any{"type": "double"}
	}
	for _, a := range intAttrs {
		properties[a] = map[string]any{"type": "long"}
	}
	for _, a := range dateAttrs {
		properties[a] = epochDate()
	}
	for _, a := range boolAttrs {
		properties[a] = map[string]any{"type": "boolean"}
	}
	for _, a := range nestedAttrs {
		properties[a] = map[string]any{"type": "nested", "dynamic": true}
	}
	return properties
}

// buildMappings returns the OpenSearch index body {settings, mappings} for the adstash schema.
func buildMappings() map[string]any {
	// Dynamic templates (first match wins), mirroring the transform's auto-rules for fields
	// that are not explicitly mapped above.
	dynamicTemplates := []map[string]any{
		matchTemplate("expr_fields", "*_EXPR", map[string]any{"type": "keyword", "index": false, "ignore_above": 256}),
		matchTemplate("date_fields", "*Date", epochDate()),
		matchTemplate("provisioned_fields", "*Provisioned", map[string]any{"type": "long"}),
		regexTemplate("request_fields", `^Request[A-Z].*$`, map[string]any{"type": "long"}),
		regexTemplate("bool_fields", `^(Want|Has|Is)[A-Z_].*$`, map[string]any{"type": "boolean"}),
		{"strings_as_keywords": map[string]any{
			"match_mapping_type": "string",
			"mapping":            map[string]any{"type": "keyword", "norms": false, "ignore_above": 256},
		}},
	}

	return map[string]any{
		"settings": map[string]any{
			"index": map[string]any{"mapping": map[string]any{"total_fields": map[string]any{"limit": 5000}}},
		},
		"mappings": map[string]any{
			"dynamic_templates": dynamicTemplates,
			"properties":        indexProperties(),
			"date_detection":    false,
			"numeric_detection": false,
		},
	}
}

func epochDate() map[string]any { return map[string]any{"type": "date", "format": "epoch_second"} }

func matchTemplate(name, pattern string, mapping map[string]any) map[string]any {
	return map[string]any{name: map[string]any{"match": pattern, "mapping": mapping}}
}

func regexTemplate(name, pattern string, mapping map[string]any) map[string]any {
	return map[string]any{name: map[string]any{"match_pattern": "regex", "match": pattern, "mapping": mapping}}
}
