-- Flexible Domain/Subject Codes Migration
-- Run: psql -h 192.168.1.97 -U fis_user -d fis_db -f 03_flexible_codes.sql

-- Domain codes table — source of truth for domain abbreviations
CREATE TABLE IF NOT EXISTS domain_codes (
    code           TEXT PRIMARY KEY,
    label          TEXT NOT NULL,
    aliases        TEXT[],
    description    TEXT,
    is_active      BOOLEAN DEFAULT TRUE,
    created_at     TIMESTAMP DEFAULT NOW()
);

-- Extend subject_codes with flexible columns
ALTER TABLE subject_codes ADD COLUMN IF NOT EXISTS parent_domain TEXT;
ALTER TABLE subject_codes ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT TRUE;
ALTER TABLE subject_codes ADD COLUMN IF NOT EXISTS sort_order INT DEFAULT 0;

-- Code migration log — tracks renames without touching filenames
CREATE TABLE IF NOT EXISTS code_migrations (
    migration_id   SERIAL PRIMARY KEY,
    old_code       TEXT NOT NULL,
    new_code       TEXT NOT NULL,
    code_type      TEXT NOT NULL,                  -- 'domain' or 'subject'
    migrated_count INT DEFAULT 0,
    migrated_at    TIMESTAMP DEFAULT NOW()
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_domain_codes_active ON domain_codes(is_active);
CREATE INDEX IF NOT EXISTS idx_subject_codes_active ON subject_codes(is_active);
CREATE INDEX IF NOT EXISTS idx_subject_codes_parent ON subject_codes(parent_domain);
CREATE INDEX IF NOT EXISTS idx_code_migrations_old ON code_migrations(old_code);
