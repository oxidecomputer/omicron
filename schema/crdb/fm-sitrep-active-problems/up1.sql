CREATE TABLE IF NOT EXISTS omicron.public.rendezvous_active_problem_created (
    problem_id UUID PRIMARY KEY,
    created_at_generation INT8 NOT NULL
);
