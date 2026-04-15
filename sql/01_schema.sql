-- FIS Database Schema
-- Run: psql -h 192.168.1.97 -U fis_user -d fis_db -f 01_schema.sql

CREATE TABLE IF NOT EXISTS files (
    file_id        SERIAL PRIMARY KEY,
    sequence_id    TEXT UNIQUE NOT NULL,          -- 000147
    original_name  TEXT NOT NULL,
    proposed_name  TEXT,
    final_name     TEXT,
    file_path      TEXT NOT NULL,
    domain         TEXT,                          -- TP, DT, EV, etc.
    subject_codes  TEXT[],                        -- {MQ, JS, RS}
    slug           TEXT,                          -- NLP-generated slug
    sha256         TEXT,                          -- 64 char content hash
    status         TEXT DEFAULT 'pending',        -- pending, confirmed, skipped, auto
    confidence     FLOAT,
    source_path    TEXT,                          -- where file originally came from
    classified_at  TIMESTAMP,
    created_at     TIMESTAMP DEFAULT NOW(),
    updated_at     TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS file_tags (
    tag_id         SERIAL PRIMARY KEY,
    file_id        INT REFERENCES files(file_id) ON DELETE CASCADE,
    tag            TEXT NOT NULL,
    source         TEXT,                          -- yake, spacy, keybert, manual
    confidence     FLOAT,
    created_at     TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS subject_codes (
    code           TEXT PRIMARY KEY,              -- MQ, JS, LG
    label          TEXT NOT NULL,                 -- Master Equation
    aliases        TEXT[],                        -- {chi equation, master eq}
    domain         TEXT NOT NULL,                 -- TP, DT, EV, ALL
    description    TEXT,
    trigger_words  TEXT[]                         -- words that activate this code
);

CREATE TABLE IF NOT EXISTS corrections (
    correction_id  SERIAL PRIMARY KEY,
    file_id        INT REFERENCES files(file_id) ON DELETE CASCADE,
    old_domain     TEXT,
    old_subjects   TEXT[],
    old_slug       TEXT,
    new_domain     TEXT,
    new_subjects   TEXT[],
    new_slug       TEXT,
    corrected_at   TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bil_events (
    event_id       SERIAL PRIMARY KEY,
    model_name     TEXT NOT NULL,                 -- web, clipboard, files, content
    features       JSONB NOT NULL,
    signal         FLOAT NOT NULL,                -- 0-1 or 0-10
    prediction     FLOAT,
    created_at     TIMESTAMP DEFAULT NOW()
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_files_domain ON files(domain);
CREATE INDEX IF NOT EXISTS idx_files_status ON files(status);
CREATE INDEX IF NOT EXISTS idx_files_sha256 ON files(sha256);
CREATE INDEX IF NOT EXISTS idx_file_tags_tag ON file_tags(tag);
CREATE INDEX IF NOT EXISTS idx_file_tags_file ON file_tags(file_id);
CREATE INDEX IF NOT EXISTS idx_subject_codes_domain ON subject_codes(domain);
CREATE INDEX IF NOT EXISTS idx_bil_events_model ON bil_events(model_name);
CREATE INDEX IF NOT EXISTS idx_corrections_file ON corrections(file_id);
