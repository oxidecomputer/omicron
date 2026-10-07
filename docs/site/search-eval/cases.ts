// Search eval cases. `top`: at least one of these URLs must be in the top K
// (default 3). `none`: the query should return nothing. With neither, a case
// only shows up when its results change between runs. Add a case when a search
// goes wrong, and run the eval before changing lib/search-api.ts.

export type Case = { q: string; top?: string[]; k?: number; none?: true; note?: string }

export const cases: Case[] = [
  // Nonsense: should return nothing
  { q: 'sdfsdf', none: true },
  { q: 'asdf', none: true },
  { q: 'qwerty', none: true },
  { q: 'xyzzy', none: true },
  { q: 'zzzz', none: true },
  { q: 'jjjjj', none: true },
  { q: 'foobarbaz', none: true },
  { q: 'sdfsdf test', none: true, note: 'all terms must match' },
  { q: 'clickhouse sdfsdf', none: true },
  { q: 'tests1234', none: true },

  // Title-ish queries
  { q: 'clickhouse', top: ['/docs/clickhouse-debugging/', '/oximeter/db/schema/'] },
  { q: 'cockroachdb upgrade', top: ['/docs/crdb-upgrades/'] },
  { q: 'flaky', top: ['/docs/flake-patterns/'] },
  { q: 'saga', top: ['/docs/demo-saga/'] },
  { q: 'authz', top: ['/docs/authz-dev-guide/', '/docs/debugging-authz/'] },
  { q: 'wicket', top: ['/wicket/'] },
  { q: 'bootstore', top: ['/bootstore/'] },
  { q: 'time sync', top: ['/docs/debugging-time-sync/'] },
  { q: 'status codes', top: ['/docs/http-status-codes/'] },
  { q: 'zone bundle', top: ['/docs/zone-bundle/'] },
  { q: 'support bundle', top: ['/support-bundle-collection/'] },
  { q: 'mupdate', top: ['/docs/mupdate-update-flow/'] },
  { q: 'tuf repo depot', top: ['/docs/tuf-artifact-replication/'] },
  {
    q: 'reconfigurator',
    top: [
      '/docs/reconfigurator/',
      '/docs/reconfigurator-dev-guide/',
      '/docs/reconfigurator-ops-guide/',
    ],
  },
  { q: 'endpoint', top: ['/docs/adding-an-endpoint/'] },
  { q: 'smf', top: ['/docs/adding-or-modifying-smf-services/'] },
  { q: 'uuid kinds', top: ['/uuid-kinds/'] },
  { q: 'simulated', top: ['/docs/how-to-run-simulated/'] },
  { q: 'live tests', top: ['/live-tests/'] },
  { q: 'sled agent', top: ['/sled-agent/'] },
  { q: 'oximeter', top: ['/oximeter/'] },
  { q: 'networking', top: ['/docs/networking/'] },
  { q: 'release engineering', top: ['/docs/releng/'] },
  { q: 'error types', top: ['/docs/error-types-and-logging/'] },
  { q: 'ls-apis', top: ['/dev-tools/ls-apis/'] },
  { q: 'sp simulator', top: ['/sp-sim/'] },
  { q: 'management gateway', top: ['/gateway/'] },
  { q: 'end-to-end', top: ['/end-to-end-tests/'] },
  { q: 'oxdb', top: ['/oximeter/db/README-oxdb-sql/'] },
  { q: 'schema migration', top: ['/schema/crdb/'], k: 5 },

  // Prefixes, as typed partway through a word
  { q: 'clickh', top: ['/docs/clickhouse-debugging/', '/oximeter/db/schema/'] },
  {
    q: 'reconfig',
    top: [
      '/docs/reconfigurator/',
      '/docs/reconfigurator-dev-guide/',
      '/docs/reconfigurator-ops-guide/',
    ],
  },
  { q: 'cockroa', top: ['/docs/crdb-debugging/', '/docs/crdb-upgrades/'] },
  { q: 'bootst', top: ['/bootstore/'] },
  { q: 'flak', top: ['/docs/flake-patterns/'] },
  { q: 'zone bun', top: ['/docs/zone-bundle/'] },

  // Stemming
  { q: 'upgrading cockroachdb', top: ['/docs/crdb-upgrades/'] },
  { q: 'debugging clickhouse', top: ['/docs/clickhouse-debugging/'] },
  { q: 'endpoints', top: ['/docs/adding-an-endpoint/'] },
  { q: 'sagas', top: ['/docs/demo-saga/'] },
  { q: 'flakes', top: ['/docs/flake-patterns/'] },
  {
    q: 'installations',
    top: ['/docs/how-to-run/', '/docs/how-to-run-simulated/', '/tools/'],
    k: 5,
    note: 'stems to "instal", so pages only say "install", which is 7/13 of the query',
  },

  // Short terms
  { q: 'sp', top: ['/sp-sim/', '/gateway/'], k: 5 },
  { q: 'db', top: ['/nexus/db-queries/src/db/', '/schema/crdb/'], k: 5 },
  { q: 'tuf', top: ['/docs/tuf-artifact-replication/'] },

  // Phrases that name a section in a long page. Indexing each section as its
  // own document is what ranks these well.
  { q: 'bad update', top: ['/docs/reconfigurator-ops-guide/'], k: 1 },
  { q: 'planning reports', top: ['/docs/reconfigurator-ops-guide/'], k: 1 },
  {
    q: 'blueprint executor',
    top: ['/docs/reconfigurator-ops-guide/', '/docs/reconfigurator-dev-guide/'],
    k: 1,
  },
  { q: 'faux mgs', top: ['/docs/reconfigurator-dev-guide/'], k: 2 },
  { q: 'failure modes', top: ['/docs/debugging-authz/'], k: 1 },
  { q: 'data path', top: ['/docs/networking/'], k: 1 },
  { q: 'virtual hardware', top: ['/docs/how-to-run/'], k: 1 },

  // Queries from a model that had only read the omicron README, asked to type
  // what a new control plane developer would search for. A separate model
  // picked the pages that answer each one from the site's titles and headings.
  // Queries it found no page for are left out.
  { q: 'run simulated omicron', top: ['/docs/how-to-run-simulated/'] },
  {
    q: 'how-to-run helios',
    top: ['/docs/how-to-run/'],
    note: 'known miss: no one section has every term, so it ranks after pages with one that does',
  },
  { q: 'omicron-dev run-all', top: ['/docs/how-to-run-simulated/'] },
  { q: 'install prerequisites', top: ['/docs/how-to-run/', '/docs/how-to-run-simulated/'] },
  { q: 'control plane architecture', top: ['/docs/control-plane-architecture/'] },
  { q: 'add external api endpoint', top: ['/docs/adding-an-endpoint/'] },
  {
    q: 'api versioning',
    top: ['/docs/control-plane-architecture/'],
    note: 'known miss: the page matches, but far down',
  },
  { q: 'dbinit.sql', top: ['/schema/crdb/'] },
  {
    q: 'diesel query datastore',
    top: ['/nexus/db-queries/src/db/'],
    note: 'known miss: the page never says "datastore"',
  },
  { q: 'omdb', top: ['/docs/reconfigurator-ops-guide/'] },
  { q: 'omdb nexus background-tasks', top: ['/docs/reconfigurator-ops-guide/'] },
  { q: 'authz polar', top: ['/docs/authz-dev-guide/'] },
  { q: 'lookup_resource', top: ['/docs/adding-an-endpoint/'] },
  { q: 'nextest flaky test', top: ['/docs/flake-patterns/'] },
  { q: 'cargo hakari', top: ['/readme/'] },
  { q: 'blueprint', top: ['/docs/reconfigurator/', '/docs/reconfigurator-dev-guide/'] },
  { q: 'inventory collection', top: ['/docs/reconfigurator-ops-guide/'] },
  {
    q: 'update system tuf repo',
    top: ['/docs/reconfigurator-ops-guide/', '/docs/tuf-artifact-replication/'],
  },
  { q: 'oximeter metrics', top: ['/oximeter/'] },
  { q: 'cockroachdb', top: ['/docs/crdb-debugging/'] },
  { q: 'log files sled', top: ['/docs/zone-bundle/', '/support-bundle-collection/'] },

  // Generated queries the labeling model found no page for. Nothing to check,
  // but what they return should still look reasonable.
  { q: 'cargo xtask openapi generate' },
  { q: 'saga' },
  { q: 'saga undo idempotent' },
  { q: 'background task' },
  { q: 'nexus integration test' },
  { q: 'test logs location' },
  { q: 'buildomat ci artifacts' },
  { q: 'expectorate' },
  { q: 'zones' },
  { q: 'internal dns service discovery' },
  { q: 'blueprint diff' },
  { q: 'rack setup service' },
  { q: 'instance start failed' },
  { q: 'vmm state machine' },
  { q: 'live migration' },
  { q: 'crucible disk' },
  { q: 'region replacement' },
  { q: 'physical disk expunge' },
  { q: 'opte vpc networking' },
  { q: 'oxql' },
  { q: 'clickhouse replicated cluster' },
  { q: 'core dump' },
  { q: 'a4x2' },
]
