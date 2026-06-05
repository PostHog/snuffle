package snuffle

import "strings"

type schemaLayout string
type logSchemaLayout string

const (
	schemaLayoutCurrent schemaLayout = "current"
	schemaLayoutPostHog schemaLayout = "posthog"

	logSchemaLayoutSnuffle logSchemaLayout = "snuffle"
	logSchemaLayoutPostHog logSchemaLayout = "posthog"
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

func storageLogSchemaLayout(value string) logSchemaLayout {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "snuffle", "current", "default", "native":
		return logSchemaLayoutSnuffle
	case "posthog", "posthog_compat", "posthog-compatible":
		return logSchemaLayoutPostHog
	default:
		return logSchemaLayoutSnuffle
	}
}

func (cfg Config) storageLogSchemaLayout() logSchemaLayout {
	return storageLogSchemaLayout(cfg.LogSchemaLayout)
}

func (cfg Config) postHogLogSchemaLayout() bool {
	return cfg.storageLogSchemaLayout() == logSchemaLayoutPostHog
}
