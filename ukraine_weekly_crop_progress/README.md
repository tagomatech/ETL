# Weekly Crop Data

This folder is the cleaned working area for Ukraine weekly crop progress and crop-condition data from `minagro.gov.ua`.

## Current State

The workflow now supports automatic auth capture and gap-aware weekly sync.

If the saved cookies are missing or stale, it can:

1. Open a visible browser window.
2. Wait for you to solve Cloudflare only if the page asks for it.
3. Detect `cf_clearance`, `minagro_session`, and `XSRF-TOKEN` itself.
4. Save them into the local-only `auth/minagro_request.txt` file.
5. Continue with the historical sync or weekly sync.

When the sync runs, it does not blindly refetch everything.

It scans the expected Friday snapshots, checks what official Minagro weeks are already covered in the CSV, and fills only the missing historical or recent weekly gaps.

You can still paste a manual cURL into a local copy based on `auth/minagro_request.example.txt` if you want, but it is no longer the main path.

The repo is intended to keep only the example auth file in version control. Live auth files, browser profiles, and logs stay local and are ignored by Git.

## Easiest Launchers

Most users can just double-click:

- `Sync Weekly Crop Data.cmd`

That is the main one-button path:

1. It opens Minagro only if auth needs to be refreshed.
2. You click the verification button only if the page asks for it.
3. It scans the CSV for missing weekly history from `2024-01-05` through today.
4. It fetches only the missing weeks.
5. It rebuilds the dashboards.

Other launchers:

- `Capture Minagro Auth.cmd`
- `Create Historical Data.cmd`
- `Update Weekly Data.cmd`

Those wrappers call the Python workflow for you.

## Commands

Capture or refresh auth automatically:

```powershell
python .\src\minagro_weekly_workflow.py capture-auth
```

One-command historical sync for a specific range:

```powershell
python .\src\minagro_weekly_workflow.py historical --from 2024-01-05 --to 2026-05-14
```

One-command weekly smart sync:

```powershell
python .\src\minagro_weekly_workflow.py weekly-update --history-from 2024-01-05 --to 2026-05-14
```

What `weekly-update` now does:

- Looks at the expected weekly Friday snapshots from `--history-from` to `--to`.
- Detects missing historical gaps inside that window.
- Detects newer missing weeks near the current end of the series.
- Uses cached raw files when available.
- Sends web requests only for the missing pieces.

Reference files only:

```powershell
python .\src\minagro_weekly_workflow.py write-reference
```

Bootstrap the processed CSV with the small public-mirror seed so the dashboards have something to open immediately:

```powershell
python .\src\minagro_weekly_workflow.py seed-public
```

Low-level backfill of an explicit range with a weekly Friday anchor:

```powershell
python .\src\minagro_weekly_workflow.py fetch --from 2024-01-05 --to 2026-05-14
```

Low-level append-only update from the last snapshot already in the processed CSV:

```powershell
python .\src\minagro_weekly_workflow.py update --to 2026-05-14
```

Rebuild the standalone dashboard from the processed CSV:

```powershell
python .\src\minagro_weekly_workflow.py build-views
```

The generated `views/executive_dashboard.html` file is intentionally treated as local build output for GitHub publishing, because the embedded standalone HTML can exceed the normal GitHub file-size limit. Rebuild it locally after cloning when needed.

Do both update and rebuild in one step:

```powershell
python .\src\minagro_weekly_workflow.py refresh --to 2026-05-14
```

If you want to prevent browser opening and rely only on the saved auth file:

```powershell
python .\src\minagro_weekly_workflow.py weekly-update --history-from 2024-01-05 --to 2026-05-14 --no-browser-auth
```

## What The Workflow Produces

- `data/processed/ukraine_weekly_crop_progress.csv`
  - Main normalized dataset for national and oblast-level observations.
- `data/reference/translation_lookup.csv`
  - English/Ukrainian label mapping exposed for audit and lookup.
- `data/reference/data_dictionary.csv`
  - Column dictionary for the processed CSV.
- `views/executive_dashboard.html`
  - Main unified one-page dashboard with a national view, clickable oblast map, regional drilldown, and season-over-season comparisons.

## English / Ukrainian Label Handling

The dashboards keep English as the primary on-screen language.

To keep the mapping transparent:

- Each dashboard includes a translation lookup panel.
- A checkbox can reveal the original Ukrainian labels beside the English display labels.
- The processed CSV preserves both Ukrainian source labels and English normalized labels.

## Condition Index

The workflow also creates a derived `Crop condition index (derived)` row for winter crop condition data when the underlying condition-share rows are available.

Weights:

- `good/excellent = 1.0`
- `satisfactory = 0.6`
- `weak or sparse = 0.2`
- `lost / no emergence = 0.0`

The formula is exposed in the `notes` column so it stays auditable.

## Server Etiquette

The fetcher is intentionally conservative:

- Weekly dates only by default.
- Raw response caching per date/category/oblast.
- Slow request pacing with jitter and retries.
- No brute-force date discovery loops.

## Legacy Files

Older exploratory notebook and scraper files were moved into `legacy/`.
