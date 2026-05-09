# Google Health Ingestion Pipeline Guide

This document describes the technical implementation of the data ingestion pipeline for Wide Lens Health, focusing on reliability, self-healing, and synchronization.

## 1. Pipeline Architecture
The ingestion engine is a series of Flask routes designed to be triggered by **Cloud Scheduler** (via Cron) or **Cloud Tasks**.

### Key Routes:
- `/google_health_heart_ingest`: Daily summaries (Min/Max/Avg/Resting).
- `/google_health_movement_ingest`: Steps, Distance, Floors, and Calories.
- `/google_health_sleep_ingest`: Sleep stages and durations.
- `/google_health_heart_intraday_ingest`: 1-minute granular heart rate samples.

---

## 2. The "High Water Mark" Strategy
To avoid redundant API calls and save costs, the pipeline determines its own starting point for every user:
1. **Lookup:** For each patient, the script queries BigQuery: `SELECT MAX(date) FROM [table] WHERE id = [email]`.
2. **Fallback:** If no data is found (new patient), it defaults to a **7-day lookback**.
3. **The Window:** It sets the ingestion `end_date` to `Yesterday`.

---

## 3. Self-Healing (The 3-Day Overlap)
Wearable data is notoriously "late." A patient may not sync their watch for several days. To handle this, we use a **3-day rolling overlap**:

1. **Overlap Calculation:** `start_date = max_date_from_bq - 3 days`.
2. **Clean Slate:** Before uploading new data, the pipeline executes a `DELETE` query for that 3-day window:
   - `DELETE FROM [table] WHERE id = [email] AND date >= [start_date]`
3. **Re-Ingest:** It then fetches and inserts fresh data for that same window.

**Result:** Any data point in BigQuery is "double-checked" for accuracy three times before the window moves past it. This automatically corrects "Sync Lag" without manual intervention.

---

## 4. Handling Gaps and Dead Batteries
If a patient stops wearing their device:
- **Detection:** The Google Health API returns an empty response for the requested range.
- **Pause:** The pipeline logs "No data found" and **stops advancing** the High Water Mark for that user.
- **Auto-Resume:** The pipeline will keep checking that gap every night. As soon as the patient syncs their device, the pipeline will "discover" the data and catch up, even if the gap was several weeks long.

---

## 5. Security & Approval Gate
Data ingestion is protected by a two-stage gate:
1. **OAuth:** The patient must provide valid Google/Fitbit consent.
2. **Researcher Approval:** The system checks the `is_active` flag in Firestore.
   - If `is_active == false`, the pipeline skips the user entirely, even if they have valid tokens. This allows researchers to control study participation and costs.
