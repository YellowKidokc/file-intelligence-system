-- Seed subject codes
-- Domain codes: TP=Theophysics, DT=Day Trading, EV=Evidence, AP=Apps,
--               MD=Media, DC=Documents, OB=Obsidian, CB=Clipboard, SY=System

INSERT INTO subject_codes (code, label, aliases, domain, description, trigger_words) VALUES
-- Theophysics subjects
('MQ', 'Master Equation', '{chi equation, master eq, χ equation, convergence kernel}', 'TP', 'The master equation χ = ∭(G·M·E·S·T·K·R·Q·F·C) and its components', '{master, equation, chi, convergence, kernel, integral, superfactor}'),
('LG', 'Logos Papers', '{logos, formal paper, publication}', 'TP', 'Published formal papers FP-001 through FP-016+', '{logos, paper, formal, publication, FP-}'),
('IS', 'Isomorphism', '{isomorphic, structural mapping}', 'TP', 'Structural mappings between physics and theology', '{isomorphism, isomorphic, mapping, structural, correspondence}'),
('JS', 'Jesus Series', '{jesus, christology, incarnation}', 'TP', 'Jesus-specific theological physics content', '{jesus, christ, incarnation, christology, messiah}'),
('SV', 'Salvation', '{soteriology, redemption theory}', 'TP', 'Salvation mechanics and soteriology', '{salvation, saved, soteriology, redemption, atonement}'),
('RS', 'Resurrection', '{rising, quantum resurrection}', 'TP', 'Resurrection physics and quantum decoherence', '{resurrection, rising, risen, decoherence, superposition}'),
('GR', 'Grace', '{grace field, reentanglement}', 'TP', 'Grace as fundamental force, reentanglement mechanics', '{grace, reentanglement, gravity, fundamental, force}'),
('CS', 'Consciousness', '{observer, qualia, awareness}', 'TP', 'Consciousness as fundamental not emergent', '{consciousness, observer, qualia, awareness, fundamental, emergent}'),
('EN', 'Entropy', '{thermodynamic, disorder, judgment}', 'TP', 'Entropy-judgment isomorphism, thermodynamic theology', '{entropy, thermodynamic, disorder, judgment, decay, heat}'),
('EV', 'Evolution/TIE', '{TIE campaign, evolution debate}', 'TP', 'Truth in Evidence campaign, evolution discussion', '{evolution, TIE, campaign, darwin, natural selection}'),
('AX', 'Axioms', '{axiom, formal axiom, tier}', 'TP', '189 axioms across 7 tiers', '{axiom, tier, formal, foundational, postulate}'),
('WV', 'Worldviews', '{worldview, philosophy, metaphysics}', 'TP', 'Worldview comparisons and metaphysical frameworks', '{worldview, philosophy, metaphysics, ontology, naturalism, theism}'),
('PH', 'Personhood', '{personhood, person, identity}', 'TP', 'Personhood tier and identity axioms', '{personhood, person, identity, self, being}'),
('TM', 'Time', '{temporal, chronology, kairos}', 'TP', 'Time as superfactor, temporal mechanics', '{time, temporal, chronology, kairos, chronos}'),
('KN', 'Knowledge', '{epistemology, knowing}', 'TP', 'Knowledge as superfactor, epistemological frameworks', '{knowledge, epistemology, knowing, epistemic}'),
('MR', 'Moral Alignment', '{moral, ethics, alignment}', 'TP', 'Moral alignment superfactor', '{moral, ethics, alignment, good, evil, righteousness}'),
('QC', 'Quantum Consciousness', '{quantum mind, observer effect}', 'TP', 'Quantum consciousness superfactor', '{quantum, mind, observer, wavefunction, collapse}'),
('FH', 'Faith', '{pistis, trust, belief}', 'TP', 'Faith as superfactor, epistemic trust', '{faith, pistis, trust, belief, confidence}'),
('CO', 'Coherence', '{coherence, decoherence, integrity}', 'TP', 'Coherence superfactor, structural integrity', '{coherence, decoherence, integrity, unity}'),
('RO', 'Redemptive Order', '{redemptive, restoration}', 'TP', 'Redemptive order superfactor', '{redemptive, restoration, order, redemption, restore}'),

-- Day Trading subjects
('ST', 'Setups', '{trade setup, entry, strategy}', 'DT', 'Trade setups and entry strategies', '{setup, entry, strategy, pattern, breakout}'),
('JR', 'Journal', '{trade journal, log, review}', 'DT', 'Trade journal entries and reviews', '{journal, log, review, recap, reflection}'),
('BT', 'Backtests', '{backtest, historical, simulation}', 'DT', 'Backtesting results and analysis', '{backtest, historical, simulation, test, validate}'),
('TK', 'Tickers', '{ticker, symbol, stock}', 'DT', 'Specific ticker analysis', '{ticker, symbol, SPY, QQQ, stock, option}'),

-- Evidence subjects
('SL', 'Sellvia', '{sellvia, ilya, dolgikh}', 'EV', 'Sellvia fraud evidence', '{sellvia, ilya, dolgikh, sunshine, fraud, bot}'),
('LW', 'Legal/Witness', '{legal, court, testimony}', 'EV', 'Legal documents and witness records', '{legal, court, testimony, witness, deposition, evidence}'),
('FD', 'Fraud', '{fraud, scam, deception}', 'EV', 'General fraud documentation', '{fraud, scam, deception, false, misleading}'),

-- General subjects
('GN', 'General', '{general, misc, uncategorized}', 'ALL', 'Catch-all for unclassified content', '{general}')

ON CONFLICT (code) DO UPDATE SET
    label = EXCLUDED.label,
    aliases = EXCLUDED.aliases,
    description = EXCLUDED.description,
    trigger_words = EXCLUDED.trigger_words;
