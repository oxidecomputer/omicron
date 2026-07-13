-- Human-readable debugging reports for a sitrep.
--
-- These are for diagnostic purposes only (i.e. for use by omdb, inclusion in
-- support bundles, etc), and are not intended as operational data.
CREATE TABLE IF NOT EXISTS omicron.public.fm_sitrep_analysis_report (
    -- The ID of the sitrep that these reports describe.
    -- This is a foreign key into the `fm_sitrep` table.
    sitrep_id UUID PRIMARY KEY,

    -- The Git commit of the Nexus that produced this report.
    git_commit STRING(64) NOT NULL,

    -- A JSON object describing the inputs to the analysis phase that produced
    -- this sitrep (collected during the analysis phase).
    input_report JSONB NOT NULL,

    -- A JSON object describing the analysis phase that produced this sitrep.
    analysis_report JSONB NOT NULL
);
