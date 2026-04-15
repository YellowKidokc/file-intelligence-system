-- Preference Engine / Link Intelligence Schema
-- Run: psql -h 192.168.1.97 -U fis_user -d fis_db -f 03_preference_engine.sql

-- Links table — every URL ingested into the system
CREATE TABLE IF NOT EXISTS links (
    link_id        SERIAL PRIMARY KEY,
    url            TEXT UNIQUE NOT NULL,
    domain         TEXT,                          -- extracted hostname (arxiv.org, youtube.com)
    title          TEXT,                          -- page title / og:title
    description    TEXT,                          -- meta description / og:description
    content_text   TEXT,                          -- extracted body text (first 50k chars)
    fis_domain     TEXT,                          -- FIS domain code (TP, DT, EV, ...)
    subject_codes  TEXT[],                        -- FIS subject codes {MQ, JS, ...}
    slug           TEXT,                          -- NLP-generated slug
    keywords       TEXT[],                        -- extracted keywords
    confidence     FLOAT,                        -- FIS classification confidence
    content_hash   TEXT,                          -- SHA256 of content_text for dedup
    source         TEXT DEFAULT 'manual',         -- manual, bulk, browser, api
    ingested_at    TIMESTAMP DEFAULT NOW(),
    classified_at  TIMESTAMP,
    created_at     TIMESTAMP DEFAULT NOW()
);

-- Preferences table — explicit like/dislike/rate for links or files
CREATE TABLE IF NOT EXISTS preferences (
    pref_id        SERIAL PRIMARY KEY,
    link_id        INT REFERENCES links(link_id) ON DELETE CASCADE,
    file_id        INT REFERENCES files(file_id) ON DELETE CASCADE,
    action         TEXT NOT NULL,                 -- like, dislike, rate
    score          FLOAT NOT NULL,               -- like=1.0, dislike=0.0, rate=0.1-1.0
    tags           TEXT[],                        -- user-supplied reason tags
    note           TEXT,                          -- optional freeform note
    fed_to_bil     BOOLEAN DEFAULT FALSE,        -- has this been used for BIL training?
    created_at     TIMESTAMP DEFAULT NOW(),
    CONSTRAINT pref_has_target CHECK (link_id IS NOT NULL OR file_id IS NOT NULL)
);

-- Taste profile — aggregated preference vectors per domain/topic
CREATE TABLE IF NOT EXISTS taste_profiles (
    profile_id     SERIAL PRIMARY KEY,
    dimension      TEXT NOT NULL,                 -- domain, subject, keyword, url_domain
    dimension_key  TEXT NOT NULL,                 -- TP, MQ, arxiv.org, etc.
    like_count     INT DEFAULT 0,
    dislike_count  INT DEFAULT 0,
    avg_score      FLOAT DEFAULT 0.5,
    total_signals  INT DEFAULT 0,
    last_updated   TIMESTAMP DEFAULT NOW(),
    UNIQUE (dimension, dimension_key)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_links_url ON links(url);
CREATE INDEX IF NOT EXISTS idx_links_domain ON links(domain);
CREATE INDEX IF NOT EXISTS idx_links_fis_domain ON links(fis_domain);
CREATE INDEX IF NOT EXISTS idx_links_source ON links(source);
CREATE INDEX IF NOT EXISTS idx_links_content_hash ON links(content_hash);
CREATE INDEX IF NOT EXISTS idx_preferences_link ON preferences(link_id);
CREATE INDEX IF NOT EXISTS idx_preferences_file ON preferences(file_id);
CREATE INDEX IF NOT EXISTS idx_preferences_action ON preferences(action);
CREATE INDEX IF NOT EXISTS idx_preferences_fed ON preferences(fed_to_bil);
CREATE INDEX IF NOT EXISTS idx_taste_dim ON taste_profiles(dimension, dimension_key);
