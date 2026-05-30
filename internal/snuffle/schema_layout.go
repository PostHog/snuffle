package snuffle

import "strings"

type schemaLayout string

const (
	schemaLayoutCurrent schemaLayout = "current"
	schemaLayoutPostHog schemaLayout = "posthog"
)

func storageSchemaLayout(value string) schemaLayout {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "current", "default", "prometheus", "prom":
		return schemaLayoutCurrent
	case "posthog", "posthog_compat", "posthog-compatible":
		return schemaLayoutPostHog
	default:
		return schemaLayoutCurrent
	}
}

func (cfg Config) storageSchemaLayout() schemaLayout {
	return storageSchemaLayout(cfg.SchemaLayout)
}

func (cfg Config) postHogSchemaLayout() bool {
	return cfg.storageSchemaLayout() == schemaLayoutPostHog
}
