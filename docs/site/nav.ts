// Structure of the Omicron developer docs site. Only the files listed here are
// published. Paths are relative to the repo root. A page's title comes from the
// document's own title unless overridden with `{ path: '...', title: '...' }`.
//
// See README.md in this directory for how the site is built and deployed.

import {
  Action16Icon,
  Cloud16Icon,
  Compass16Icon,
  Repair16Icon,
  Servers16Icon,
  SoftwareUpdate16Icon,
} from '@oxide/design-system/icons/react'

import type { Site } from './lib/types.ts'

export const site: Site = {
  title: 'Omicron',
  tagline: 'developer docs',
  repo: 'https://github.com/oxidecomputer/omicron',
  branch: 'main',
  sections: [
    {
      title: 'Getting started',
      icon: Compass16Icon,
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
      icon: Servers16Icon,
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
      icon: Cloud16Icon,
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
      icon: SoftwareUpdate16Icon,
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
      icon: Action16Icon,
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
      icon: Repair16Icon,
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
