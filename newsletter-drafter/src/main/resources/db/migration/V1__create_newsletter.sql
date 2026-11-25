-- Application table (the Quarkus Flow runtime schema lives in db/flow-migration, with its own history table)
CREATE TABLE newsletter
(
    instance_id  VARCHAR(26)                 NOT NULL,
    status       VARCHAR(32)                 NOT NULL,
    current_step VARCHAR(64),
    mood         VARCHAR(16),
    tone         VARCHAR(16),
    length       VARCHAR(16),
    top_movers   VARCHAR(1024),
    macro_data   TEXT,
    title        TEXT,
    lead         TEXT,
    body         TEXT,
    notes        TEXT,
    created_at   TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    updated_at   TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    PRIMARY KEY (instance_id)
);

CREATE INDEX newsletter_created_at_idx ON newsletter (created_at DESC);
