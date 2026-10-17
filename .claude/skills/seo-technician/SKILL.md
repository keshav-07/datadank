---
name: seo-technician
description: Continue DataDank's SEO and ranking work. Use when asked to improve SEO, rankings, Search Console results, page speed, structured data, internal linking or to "continue the SEO to-do list". Holds the live to-do list, audit results and how to re-run each check.
---

# DataDank SEO technician

Live state of the SEO programme for https://datadank.com (Astro static site on Cloudflare). Read this first,
continue from the first unchecked item, and **update this file and push after every finished step** (the
user asked for this so work survives running out of tokens). Log weekly Search Console numbers in
`docs/seo/SEO_LOG.md`; the keyword-to-URL map is `docs/seo/KEYWORD_URL_MAP.md`.

Honest framing for the user: no one can promise "rank faster than any site". The site is new and the apex
domain is on some vendors' blocklists from a previous owner. Remove blockers, win long-tail queries first,
earn links.

## Ground rules

- Do not change the 90-day planner content or regenerate `docs/curriculum/plan-90-days.json` (user decision,
  17 Oct 2026). The only allowed planner change is repointing a broken link anchor.
- Host: keep `https://datadank.com` for now. A `www` switch is on hold while the user clears the vendor
  blocklist (a ready patch was discarded; redo it from scratch if asked: `astro.config.mjs` default site,
  `src/utils/seo.ts` fallback, `scripts/check-dist.mjs` ORIGIN, docs).
- No FAQ schema (Google shows FAQ rich results only for government and health sites). No QAPage schema
  (that is for user-generated Q&A).
- Validate with `npm run validate` before every push. Commit messages end with the Co-Authored-By and
  Claude-Session lines from the session's attribution reminder.

## To-do list

Owner: **U** = user, **C** = Claude.

### P0 blockers
- [ ] U1. Clear vendor blocklists (VirusTotal → each flagging vendor's review form).
- [ ] U2. Search Console: Security & Manual Actions clean; resubmit `https://datadank.com/sitemap-index.xml`;
      request indexing for ~20 key URLs (home, 11 Top-N articles, /interview/questions/, course hubs, planner).
- [ ] U3. One host: `www.datadank.com` serves 200; 301 it (or the apex) once the vendor issue is settled.
- [ ] U4. Bing Webmaster Tools: import from Search Console; enable IndexNow / Cloudflare Crawler Hints.
- [ ] U5. GA4 Realtime shows visits.

### P1 technical (Claude)
- [x] C6. Lighthouse on key pages (17 Oct 2026, local build, mobile): see "Audit results".
- [ ] C6a. Fix accessibility findings: planner checkbox 22px → 24px target; code-comment colour #6A737D on
      #161616 (3.75:1) in the Shiki theme; phase-card number #ff6a00 on #f8f5ef (2.63:1, decorative).
- [ ] C6b. Home page TBT 260 ms: the day card parses JSON for all 90 days on load; defer or slim it.
- [x] C7a. Structured data audit: BreadcrumbList on 699 pages, Article on 644, WebSite + Organization on home.
- [ ] C7b. Add `Course` schema (provider, offers Free, hasCourseInstance online with workload) to the 15
      course hubs (`src/pages/[tech]/index.astro`).
- [ ] C7c. Article author is the Organization "DataDank Editorial"; a named person with a bio page would
      help E-E-A-T. Ask the user for an author name before changing.
- [x] C10. Titles and descriptions: no duplicates, no title over 60 characters; 2 descriptions over 165
      (`/delta-lake/schema-evolution-patterns/` 167, `/interview/sql/index-not-used-explain/` 169).
- [ ] C10a. Shorten those 2 descriptions.
- [ ] C8. Pages reachable only via listings: `interview-questions:delta-lake/parquet-internals`,
      `roadmaps:switching-to-data-engineering`; add a contextual link to each.
- [ ] C9. Broken-link and redirect audit across all pages (internal + external).
- [ ] C11. Sitemap lastmod only changes with real content changes (check `astro.config.mjs` serialize).
- [ ] C12. Per-page share images: every page uses `/og/default.png`. Generate per-course (or per-article)
      OG images at build time.

### P2 content (Claude)
- [ ] C13. Long-tail targets first (see KEYWORD_URL_MAP Tier 1).
- [ ] C14. Comparison pages: Snowflake vs Databricks, Kafka vs Kinesis, Delta vs Iceberg, ETL vs ELT,
      Airflow vs Dagster vs Prefect, Parquet vs Avro vs ORC (check existing pages first to avoid cannibalising).
- [ ] C15. Kafka bank to Top 50 (writer agent drafting Q509–Q518; when merged, keep planner as is and only fix
      Kafka anchors in the plan), then Top-N pages for AWS, Delta Lake, system design.
- [ ] C16. Topic-cluster link check: hub ↔ lessons ↔ questions ↔ Top-N.
- [ ] C17. Quarterly freshness review of version-sensitive pages.
- [ ] C18. Author bio and trust signals (with C7c).

### P3 authority (mostly user)
- [ ] U19. Public GitHub repo (e.g. data-engineering-interview-questions) linking to the site.
- [ ] U20. Daily LinkedIn posts linking one page each (linkedin-de-daily-post skill).
- [ ] U21. Cross-post 10 best articles to dev.to / Medium / Hashnode with canonical to DataDank.
- [ ] U22. Helpful answers on r/dataengineering, Stack Overflow, Quora (no spam).
- [ ] U23. Submit to awesome-data-engineering lists; newsletter link swaps.
- [ ] U24. Use the DataDank name everywhere (branded search).

### P4 measure (weekly, from ~2 weeks after indexing)
- [ ] C25. Search Console review → SEO_LOG.md: low-CTR pages (retitle), positions 8–30 (strengthen),
      unindexed pages (reason).
- [ ] C26. Cannibalisation check (watch list in KEYWORD_URL_MAP).
- [ ] C27. Monthly report.

## Audit results

### 17 Oct 2026, Lighthouse 12 (mobile, local `astro preview`)

| Page | Perf | A11y | Best pr. | SEO | LCP | TBT | CLS | DOM |
|---|---|---|---|---|---|---|---|---|
| `/` | 94 | 93 | 96 | 100 | 2.0 s | 260 ms | 0 | 800 |
| `/sql/sql-fundamentals/` | 100 | 97 | 96 | 100 | 1.7 s | 0 | 0 | 2,289 |
| `/sql/sql-interview-questions/` | 99 | 100 | 96 | 100 | 1.8 s | 0 | 0 | 8,786 |
| `/planner/90-day-plan/` | 99 | 97 | 96 | 100 | 2.0 s | 20 ms | 0 | 8,182 |
| `/interview/questions/` | 96 | 100 | 96 | 100 | 2.1 s | 170 ms | 0 | 13,035 |
| `/sql/` | 100 | 100 | 96 | 100 | 1.7 s | 0 | 0 | 890 |

Best-practices 96 everywhere is only the sandbox blocking Google Analytics (`ERR_TUNNEL_CONNECTION_FAILED`),
not a site problem. Failing a11y audits: listed under C6a.

### 17 Oct 2026, meta and structured data (all 701 built pages)

700 indexable (only `/search/` is noindex), 1 H1 each, no duplicate titles or descriptions, every page has
`og:image` (the same default) and `summary_large_image`. JSON-LD: BreadcrumbList 699, Article 644, WebSite 1,
Organization 1. Course hubs have only BreadcrumbList.

## How to re-run the checks

```bash
cd /home/user/data-career-hub && npm run build
# Lighthouse (install once into the scratchpad, never into the repo)
npm i --no-save --prefix "$SCRATCH/lh" lighthouse@12
npx astro preview --port 4321 &
CHROME_PATH=/opt/pw-browsers/chromium "$SCRATCH/lh/node_modules/.bin/lighthouse" http://localhost:4321/ \
  --quiet --chrome-flags="--headless=new --no-sandbox" --output=json --output-path=report.json
```

Meta/structured-data audit: walk `dist/**/index.html`, read `<title>`, `meta[name=description]`,
`og:image`, `<h1>` count and each `application/ld+json` block (a short Python script; see git history of this
file for the one used on 17 Oct).
