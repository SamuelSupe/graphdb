package main

import "gitlab.jiagouyun.com/guance/graphdb/internal/storage"

type commandKind uint8

const (
	commandRegular commandKind = iota
	commandServe
	commandCoordinator
	commandVersion
	commandHelp
)

type commandHandler func([]string, *storage.TenantStore) error

type commandSpec struct {
	name     string
	usage    []string
	kind     commandKind
	mutation bool
	handler  commandHandler
}

var commandSpecs = []commandSpec{
	{name: "serve", usage: []string{"graphdb serve"}, kind: commandServe},
	{name: "version", usage: []string{"graphdb version"}, kind: commandVersion},
	{name: "coordinator", usage: []string{
		"graphdb coordinator migrate",
		"graphdb coordinator bootstrap --dry-run|--apply",
		"graphdb coordinator status",
		"graphdb coordinator sync-legacy-manifest",
		"graphdb coordinator rollback --dry-run",
		"graphdb coordinator rollback --apply --writers-stopped",
	}, kind: commandCoordinator},
	{name: "init-tenant", usage: []string{"graphdb init-tenant <tenant-id>"}, mutation: true, handler: initTenant},
	{name: "list-tenants", usage: []string{"graphdb list-tenants"}, handler: listTenants},
	{name: "tenant", usage: []string{"graphdb tenant <tenant-id>"}, handler: tenantInfo},
	{name: "create-tenant", usage: []string{"graphdb create-tenant <tenant-id> [metadata.json]"}, mutation: true, handler: createTenant},
	{name: "set-tenant-metadata", usage: []string{"graphdb set-tenant-metadata <tenant-id> <metadata.json>"}, mutation: true, handler: setTenantMetadata},
	{name: "disable-tenant", usage: []string{"graphdb disable-tenant <tenant-id>"}, mutation: true, handler: disableTenant},
	{name: "enable-tenant", usage: []string{"graphdb enable-tenant <tenant-id>"}, mutation: true, handler: enableTenant},
	{name: "delete-tenant", usage: []string{"graphdb delete-tenant <tenant-id>"}, mutation: true, handler: deleteTenant},
	{name: "purge-tenant", usage: []string{"graphdb purge-tenant <tenant-id> [--force]"}, mutation: true, handler: purgeTenant},
	{name: "clone-tenant", usage: []string{"graphdb clone-tenant <source-tenant-id> <target-tenant-id> [metadata.json]"}, mutation: true, handler: cloneTenant},
	{name: "backup-tenant", usage: []string{"graphdb backup-tenant <tenant-id>"}, mutation: true, handler: backupTenant},
	{name: "restore-tenant", usage: []string{"graphdb restore-tenant <tenant-id> <backup-key> [--overwrite] [--dry-run]"}, mutation: true, handler: restoreTenant},
	{name: "restore-drill-tenant", usage: []string{"graphdb restore-drill-tenant <tenant-id> [params.json]"}, mutation: true, handler: restoreDrillTenant},
	{name: "commit", usage: []string{"graphdb commit <tenant-id> <commit.json>"}, mutation: true, handler: commit},
	{name: "ingest", usage: []string{"graphdb ingest <tenant-id> <ingest.json>"}, mutation: true, handler: ingest},
	{name: "collector-status", usage: []string{"graphdb collector-status <tenant-id> <source> <collector-id>"}, handler: collectorStatus},
	{name: "source-policy", usage: []string{"graphdb source-policy <tenant-id>"}, handler: sourcePolicy},
	{name: "set-source-policy", usage: []string{"graphdb set-source-policy <tenant-id> <policy.json>"}, mutation: true, handler: setSourcePolicy},
	{name: "tenant-config", usage: []string{"graphdb tenant-config <tenant-id>"}, handler: tenantConfig},
	{name: "set-tenant-config", usage: []string{"graphdb set-tenant-config <tenant-id> <config.json>"}, mutation: true, handler: setTenantConfig},
	{name: "tenant-usage", usage: []string{"graphdb tenant-usage <tenant-id>"}, handler: tenantUsage},
	{name: "deadletters", usage: []string{"graphdb deadletters <tenant-id> <source>"}, handler: deadLetters},
	{name: "replay-deadletters", usage: []string{"graphdb replay-deadletters <tenant-id> <source> [limit]"}, mutation: true, handler: replayDeadLetters},
	{name: "query", usage: []string{"graphdb query <tenant-id> <query.json>"}, handler: runQuery},
	{name: "graphql", usage: []string{"graphdb graphql <tenant-id> <graphql-request.json>"}, handler: runGraphQL},
	{name: "gql", usage: []string{"graphdb gql <tenant-id> <legacy-query.gql> (deprecated legacy text DSL)"}, handler: runGQL},
	{name: "save-query", usage: []string{"graphdb save-query <tenant-id> <saved-query.json>"}, mutation: true, handler: saveQuery},
	{name: "list-queries", usage: []string{"graphdb list-queries <tenant-id>"}, handler: listQueries},
	{name: "run-saved-query", usage: []string{"graphdb run-saved-query <tenant-id> <name>"}, handler: runSavedQuery},
	{name: "start-task", usage: []string{"graphdb start-task <tenant-id> <type> [params.json]"}, mutation: true, handler: startTask},
	{name: "list-tasks", usage: []string{"graphdb list-tasks <tenant-id> [type] [status]"}, handler: listTasks},
	{name: "task", usage: []string{"graphdb task <tenant-id> <task-id>"}, handler: getTask},
	{name: "cancel-task", usage: []string{"graphdb cancel-task <tenant-id> <task-id>"}, mutation: true, handler: cancelTask},
	{name: "retry-task", usage: []string{"graphdb retry-task <tenant-id> <task-id>"}, mutation: true, handler: retryTask},
	{name: "index-catalog", usage: []string{"graphdb index-catalog <tenant-id>"}, handler: indexCatalog},
	{name: "index-inspect", usage: []string{"graphdb index-inspect <tenant-id>"}, handler: indexInspect},
	{name: "index-definitions", usage: []string{"graphdb index-definitions <tenant-id>"}, handler: indexDefinitions},
	{name: "create-index", usage: []string{"graphdb create-index <tenant-id> <kind> <field> [name]"}, mutation: true, handler: createIndex},
	{name: "drop-index", usage: []string{"graphdb drop-index <tenant-id> <name>"}, mutation: true, handler: dropIndex},
	{name: "index-health", usage: []string{"graphdb index-health <tenant-id>"}, handler: indexHealth},
	{name: "integrity-audit", usage: []string{"graphdb integrity-audit <tenant-id> [--shallow]"}, handler: integrityAudit},
	{name: "rebuild-indexes", usage: []string{"graphdb rebuild-indexes <tenant-id>"}, mutation: true, handler: rebuildIndexes},
	{name: "writer-lease", usage: []string{"graphdb writer-lease <tenant-id>"}, handler: writerLease},
	{name: "recover", usage: []string{"graphdb recover <tenant-id>"}, mutation: true, handler: recoverTenant},
	{name: "repair", usage: []string{"graphdb repair <tenant-id> [--apply]"}, mutation: true, handler: repairTenant},
	{name: "cleanup-commits", usage: []string{"graphdb cleanup-commits <tenant-id>"}, mutation: true, handler: cleanupCommits},
	{name: "gc", usage: []string{"graphdb gc <tenant-id> [deadletter-max-age-seconds] [task-max-age-seconds]"}, mutation: true, handler: runGC},
	{name: "compact", usage: []string{"graphdb compact <tenant-id>"}, mutation: true, handler: compact},
}

func findCommand(name string) (commandSpec, bool) {
	switch name {
	case "--version":
		name = "version"
	case "help", "-h", "--help":
		return commandSpec{name: "help", kind: commandHelp}, true
	}
	for _, command := range commandSpecs {
		if command.name == name {
			return command, true
		}
	}
	return commandSpec{}, false
}

func (c commandSpec) mayWrite(mode string) bool {
	if c.kind == commandServe {
		return mode == "all" || mode == "writer"
	}
	return c.mutation
}
