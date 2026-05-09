# Patient Onboarding & Approval Process
**App:** Wide Lens Health
**Protocol:** Approval-Gated Ingestion

## 1. Initial Authentication (Patient)
- The patient logs into the **Wide Lens Health** application.
- They complete the Fitbit/Google Health OAuth consent flow.
- **System Action:** The `fitbit_auth` logic registers the user.
- **Firestore State:** A document is created with `is_active: false`.
- **Result:** Ingestion is blocked. No data is pulled or stored in BigQuery.

## 2. Researcher Verification (Portal)
- The medical researcher logs into the **Research Portal**.
- They see the new patient in a "Pending" or "Inactive" state.
- **Researcher Action:** Review patient eligibility and set `care_status = 1` (Active).
- **Portal Action:** The app updates Cloud SQL AND sets the Firestore `is_active` flag to `true`.

## 3. Ingestion Lifecycle (Automation)
- **Nightly Run:** The `fitbit_ingest` script iterates through users.
- **Logic:** It calls `storage.get_active_status(email)`.
- **Execution:**
    - If `true`: The pipeline pulls heart, sleep, and movement data.
    - If `false`: The pipeline skips the user and logs the reason.

## 4. Study Completion or Suspension
- If a patient finishes the study or is suspended, the researcher updates `care_status`.
- **System Action:** Firestore is updated to `is_active: false`.
- **Result:** Ingestion stops immediately.
- **Data Persistence:** Historical data remains in BigQuery for analysis, but no new "expensive" intraday pulls are performed.
