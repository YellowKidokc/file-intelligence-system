# File Intelligence System (FIS)

NLP-powered file classification, renaming, and behavioral learning system. Automatically classifies files using lightweight NLP engines, proposes intelligent filenames, learns from your corrections, and builds a searchable intelligence layer backed by Postgres.

## Architecture

```
FILE ARRIVES (watchdog / hotkey / backfill)
       |
   [ROUTER by file type]
   /      |       |       \
YAKE    spaCy   Whisper   KeyBERT
(always) (always) (media)  (low confidence)
   \      |       |       /
   [CLASSIFIER + LEARNING LAYER]
   assigns: domain + subject + slug + confidence
       |
   confidence > 85  --> auto-rename silently
   confidence 50-85 --> propose in queue
   confidence < 50  --> kickout list
       |
   Postgres stores everything
   .fis_meta.json written to folder
```

## Filename Schema

```
[NLP-slug]_[DOMAIN].[SUBJECT]_[SEQ-ID].ext
```

- **Slug**: Top 3 YAKE keyphrases, kebab-case, 20 char max
- **Domain**: 2-letter code (TP, DT, EV, AP, MD, DC, OB, CB, SY)
- **Subject**: 2-letter code (MQ, LG, JS, IS, SV, RS, GR, CS, EN, AX, WV, etc.)
- **Seq ID**: 6-digit auto-incrementing global integer from Postgres
- **Date**: NOT in filename — stored as metadata in Postgres

### Examples

```
consciousness-substrate_TP.CS_000147.md
master-equation-nodrift_TP.MQ_000148.pdf
sellvia-bot-traffic_EV.SL_000151.xlsx
spy-0dte-theta-entry_DT.ST_000152.xlsx
resurrection-quantum_TP.JS-RS_000153.mp4
```

## Components

| Component | Tool | Purpose |
|-----------|------|---------|
| File watcher | Python `watchdog` | Monitor folders, trigger pipeline |
| NLP pipeline | YAKE + spaCy + KeyBERT | Extract keywords, entities, semantics |
| Audio/video | faster-whisper | Transcribe media to text |
| Classifier | scikit-learn SGD | Map NLP features to domain/subject codes |
| Learning layer | scikit-learn online | Update from your corrections |
| Database | PostgreSQL | Master intelligence layer |
| Popup UI | PySide6 | Ctrl+Alt+F rename queue + code search |
| Hotkey | AHK | Triggers popup |
| Backfill | CLI script | Batch process existing folders |
| Folder metadata | .fis_meta.json | Directory Opus integration |
| BIL | River | Behavioral Intelligence Layer |

## BIL — Behavioral Intelligence Layer

Standalone learning engine that sits on top of FIS. Learns from your behavior (bookmarks, clipboard, file access patterns) without manual labeling. Pipes predictions into FIS, Truth Engine, clipboard, and daily AI session digests.

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Configure
cp config/settings.example.ini config/settings.ini
# Edit settings.ini with your Postgres credentials and watch folders

# 3. Initialize database
python -m fis.db.init_db

# 4. Seed subject codes
python -m fis.db.seed_codes

# 5. Start the watcher
python -m fis.watcher

# 6. (Optional) Run backfill on existing folders
python -m fis.backfill --path "O:\_Theophysics_v3"
```

## Hotkeys

| Key | Action |
|-----|--------|
| Ctrl+Alt+F | Open rename queue popup |
| Ctrl+Alt+S | Force scan current folder |
| Ctrl+Alt+K | Export kickouts to Excel |
| Ctrl+Alt+B | Run backfill on selected folder |

## Resource Usage

| Engine | RAM | CPU | Notes |
|--------|-----|-----|-------|
| YAKE | ~50MB | Near zero | Pure math, instant |
| spaCy (en_core_web_sm) | ~500MB | Low | Loaded once, stays resident |
| KeyBERT (Model2Vec) | ~400MB | Low | Only fires on low confidence |
| faster-whisper (small, int8) | ~2GB VRAM | Medium | Only for audio/video, job queue |
| River | ~20MB | Near zero | Online learning, always on |

## License

MIT
