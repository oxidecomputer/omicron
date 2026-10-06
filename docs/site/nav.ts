// Structure of the Omicron developer docs site. Only the files listed here are
// published. Paths are relative to the repo root. A page's title comes from the
// document's own title unless overridden with `{ path: '...', title: '...' }`.
//
// See README.md in this directory for how the site is built and deployed.

import type { Site } from './lib/types.ts'

export const site: Site = {
  title: 'Omicron',
  description:
    'Developer documentation for the Oxide control plane: how it is put together, how to run it, and how to work on it.',
  repo: 'https://github.com/oxidecomputer/omicron',
  sections: [
    {
      title: 'Getting started',
      description: 'Build, run, and find your way around the repo.',
      pages: [
        { path: 'README.adoc', title: 'Overview' },
        'docs/repo.adoc',
        'docs/how-to-run-simulated.adoc',
        'docs/how-to-run.adoc',
        'docs/cli.adoc',
        'tools/README.adoc',
      ],
    },
    {
      title: 'Architecture',
      description: 'The major components of the control plane and how they fit together.',
      pages: [
        'docs/control-plane-architecture.adoc',
        'docs/networking.adoc',
        'sled-agent/README.adoc',
        'gateway/README.adoc',
        'wicket/README.md',
        'bootstore/README.adoc',
        'nexus/db-queries/src/db/README.adoc',
        'oximeter/README.md',
        'support-bundle-collection/README.md',
      ],
    },
    {
      title: 'Working on Nexus',
      description: 'Conventions and guides for adding to the control plane API server.',
      pages: [
        'docs/adding-an-endpoint.adoc',
        'docs/http-status-codes.adoc',
        'docs/error-types-and-logging.adoc',
        'docs/demo-saga.adoc',
        'docs/authz-dev-guide.adoc',
        'schema/crdb/README.adoc',
        'oximeter/db/schema/README.md',
        'uuid-kinds/README.adoc',
        'generation-kinds/README.adoc',
        'dev-tools/ls-apis/README.adoc',
      ],
    },
    {
      title: 'Reconfigurator and update',
      description: 'How the system plans and executes changes to its own deployment.',
      pages: [
        'docs/reconfigurator.adoc',
        'docs/reconfigurator-dev-guide.adoc',
        'docs/reconfigurator-ops-guide.adoc',
        'docs/mupdate-update-flow.adoc',
        'docs/tuf-artifact-replication.adoc',
        'docs/releng.adoc',
        'dev-tools/reconfigurator-exec-unsafe/README.adoc',
      ],
    },
    {
      title: 'Testing',
      pages: [
        'docs/flake-patterns.adoc',
        'live-tests/README.adoc',
        'end-to-end-tests/README.adoc',
        'illumos-utils/src/fakes/README.adoc',
        'sp-sim/README.adoc',
      ],
    },
    {
      title: 'Debugging and operations',
      description: 'Diagnosing problems in running systems.',
      pages: [
        'docs/debugging-authz.adoc',
        'docs/crdb-debugging.adoc',
        'docs/crdb-upgrades.adoc',
        'docs/clickhouse-debugging.adoc',
        'oximeter/db/README-oxdb-sql.md',
        'docs/debugging-time-sync.adoc',
        'docs/zone-bundle.adoc',
        'docs/adding-or-modifying-smf-services.adoc',
      ],
    },
  ],
}
