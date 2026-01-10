--This creates the table in PostgreSQL to house strava activity data.

-- Table: public.strava_activities

-- DROP TABLE IF EXISTS public.strava_activities;

CREATE TABLE IF NOT EXISTS public.strava_activities
(
    id bigint NOT NULL,
    name text COLLATE pg_catalog."default",
    distance real,
    moving_time integer,
    elapsed_time integer,
    total_elevation_gain real,
    type text COLLATE pg_catalog."default",
    sport_type text COLLATE pg_catalog."default",
    start_date_local date,
    timezone text COLLATE pg_catalog."default",
    kudos_count smallint,
    athlete_count smallint,
    commute boolean,
    manual boolean,
    gear_id text COLLATE pg_catalog."default",
    start_latlng text COLLATE pg_catalog."default",
    average_speed real,
    activity_geometry geometry(Geometry,4326),
    CONSTRAINT strava_activities_pkey PRIMARY KEY (id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.strava_activities
    OWNER to postgres;

COMMENT ON TABLE public.strava_activities
    IS 'This houses each strava acitivity and the associated geographic geometry (if applicable).';
-- Index: strava_activities_geom_idx

-- DROP INDEX IF EXISTS public.strava_activities_geom_idx;

CREATE INDEX IF NOT EXISTS strava_activities_geom_idx
    ON public.strava_activities USING gist
    (activity_geometry)
    WITH (fillfactor=90, buffering=auto)
    TABLESPACE pg_default;