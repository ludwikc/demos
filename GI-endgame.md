# DP-700 Lab — Daily Revenue Load to a Fabric Warehouse

> **Course:** DP-700 — Implementing Data Engineering Solutions Using Microsoft Fabric
> **Module:** Ingest and transform batch data / Orchestrate processes
> **Estimated duration:** 60 minutes

---

## 1. What you will build

In this lab you build a small but complete, production-shaped workflow in Microsoft Fabric:

1. **Two data sources**
   - **Source A — built-in sample:** *NYC Taxi – Green* (provided by Fabric as sample data in the Copy job/Copy assistant).
   - **Source B — public CSV:** a vendor-discount table published on GitHub, fetched via HTTPS.
2. **One simple transformation** in Dataflow Gen2 (filter, unpivot, merge, custom column).
3. **One destination:** a Fabric **Warehouse** table ready for Power BI.
4. **One scheduler:** a Data Factory **pipeline** running on a daily fixed schedule.

### Architecture

```
┌──────────────────────────────┐
│  Source A                    │
│  Sample data: NYC Taxi-Green │──┐
│  (via Copy activity)         │  │
└──────────────────────────────┘  │
                                  │          ┌────────────────────────────┐
                                  ├─► Dataflow Gen2 ──► Warehouse table ──│►  Power BI
                                  │  (filter, unpivot,   nyc_taxi_revenue │
                                  │   merge, custom col)                  │
┌──────────────────────────────┐  │          └────────────────────────────┘
│  Source B                    │  │
│  Public CSV: vendor discounts│──┘
│  (raw.githubusercontent.com) │
└──────────────────────────────┘

              Pipeline orchestrates both and runs on a daily schedule
```

---

## 2. Learning objectives (mapped to DP-700 skills measured)

By the end of this lab you will have practiced the following objectives from the **official DP-700 skills outline** (*Ingest and transform data* and *Implement and manage an analytics solution* domains):

- **Ingest data by using pipelines** — Copy activity with a sample source into a Warehouse table.
- **Choose between Dataflows Gen2, notebooks, KQL, and T-SQL for data transformation** — justify why Dataflow Gen2 fits this scenario.
- **Ingest and transform batch data** — apply Power Query steps (filter, unpivot, merge, custom column).
- **Design and implement schedules and event-based triggers** — attach a fixed daily schedule to a pipeline.
- **Monitor data ingestion / Monitor data transformation** — read the run history of both the pipeline and the dataflow.

---

## 3. Prerequisites

Before you start, confirm you have:

- A Fabric-enabled workspace (Trial capacity or F2+ works — not *My workspace*) where you are **Member** or **Admin**.
- Access to `https://app.fabric.microsoft.com`.
- Network access to `raw.githubusercontent.com` from the Fabric data plane (this is public, no gateway required).

If any student is missing the workspace, spend the first 5 minutes creating one (**+ New workspace → assign to Trial/F-SKU capacity**) before starting the clock.

---

## 4. Scenario (read this to the class)

> You are a junior data engineer at **Green Cab NYC**. Finance publishes a small CSV each day with the vendor discount applied to each taxi vendor's trips, broken out by date. The analytics team wants a single, refreshed **Warehouse** table that already joins taxi trips with the right daily discount, so Power BI can show "revenue after discount" without any extra SQL.
>
> The real taxi feed is not ready yet, so for now you will stand in with the **NYC Taxi – Green** sample data that ships with Fabric. Your deliverable is:
>
> 1. A Warehouse table called `nyc_taxi_revenue` containing trip + discount + `TotalAfterDiscount`.
> 2. A pipeline that refreshes it **every day at 06:00** your local time.

---

## 5. Time budget

| # | Task                                               | Minutes |
|---|----------------------------------------------------|--------:|
| 1 | Create workspace items (Warehouse)                 | 5       |
| 2 | Ingest Source A (NYC Taxi – Green) via Copy job    | 10      |
| 3 | Build Dataflow Gen2 with Source B + transform      | 25      |
| 4 | Wrap in a pipeline and schedule it                 | 15      |
| 5 | Verify in the Warehouse and review monitoring      | 5       |
|   | **Total**                                          | **60**  |

---

## 6. Lab steps

### Task 1 — Create the Warehouse (5 min)

1. Open your Fabric workspace.
2. Select **+ New item → Warehouse**.
3. Name it `WH_GreenCab` and select **Create**.
4. Wait for provisioning to complete. Leave the Warehouse tab open — you will reference it later.

> **Instructor note:** There is no need to pre-create tables. The Copy job and the Dataflow will auto-create destination tables.

---

### Task 2 — Ingest Source A with a Copy job (10 min)

You will use the **Copy job** item to pull the *NYC Taxi – Green* sample straight into your Warehouse.

1. In the workspace, select **+ New item → Copy job**. Name it `CJ_Load_NYC_Taxi_Green` and select **Create**.
2. On the **Choose data source** page, select **Sample data** from the ribbon and pick **NYC Taxi – Green**. Select **Next**.
3. On **Choose data destination**, select the **OneLake** tab, pick your `WH_GreenCab` warehouse, and select **Next**.
4. On **Choose copy job mode**, select **Full copy** and **Next**.
5. Name the destination table `bronze_nyc_taxi_green`. Leave column mappings as-is. Select **Next**.
6. On **Review + save**, leave **Start data transfer immediately** checked and select **Save + Run**.
7. Watch the **Results** tab until the status is **Succeeded**.

> ✅ **Checkpoint:** Open `WH_GreenCab` → refresh the Explorer → you should see the `bronze_nyc_taxi_green` table populated.

---

### Task 3 — Build the Dataflow Gen2 (25 min)

This is the heart of the lab. The dataflow reads the Warehouse table you just created, reads the discounts CSV, merges them, and writes the result back to the Warehouse.

#### 3a. Create the dataflow

1. In the workspace, select **+ New item → Dataflow Gen2**. Name it `DF_Transform_Taxi_Revenue`.
2. When the editor opens, confirm **View → Diagram view** is enabled (toggle at the bottom-right if not).

#### 3b. Add Source A — the bronze Warehouse table

1. Select **Get data → More…**
2. Search for **Warehouse** and select the **Warehouse** connector.
3. Sign in, select **Next**, browse to your `WH_GreenCab` warehouse, expand it, and select the `bronze_nyc_taxi_green` table. Select **Create**.
4. Rename the query (in **Query settings** on the right) from `bronze_nyc_taxi_green` to `Trips`.

#### 3c. Transform the Trips query

Apply a **simple, DP-700-level** set of transformations:

1. Click the data-type icon on **lpepPickupDatetime** and change it from `Date/Time` → `Date`.
2. On the **Home** tab → **Manage columns → Choose columns**, keep **only**:
   - `vendorID`
   - `lpepPickupDatetime`
   - `passengerCount`
   - `tripDistance`
   - `totalAmount`
3. Use the filter arrow on **lpepPickupDatetime** → **Date filters → Between…** and set `2015-01-01` to `2015-01-31`. Select **OK**.
4. Use the filter arrow on **passengerCount** → **Number filters → Greater than…** and enter `0`. Select **OK**.

> 📝 **Why these transformations?** Column pruning and row filtering are the classic *reduce-before-merge* pattern — you want the merge to run over as little data as possible. This is a core DP-700 good practice.

#### 3d. Add Source B — the vendor discounts CSV

1. On the **Home** tab, select **Get data → Text/CSV**.
2. In the **Connect to data source** dialog, enter:

   | Field                  | Value                                                                                                 |
   |------------------------|-------------------------------------------------------------------------------------------------------|
   | **File path or URL**   | `https://raw.githubusercontent.com/ekote/azure-architect/master/Generated-NYC-Taxi-Green-Discounts.csv` |
   | **Authentication kind**| `Anonymous`                                                                                           |

3. Select **Next → Create**.

#### 3e. Transform the Discounts query

1. In the preview grid, open the table context menu (top-left of the grid) → **Use first row as headers**.
2. Right-click the **VendorID** column → **Unpivot other columns**. This reshapes the wide daily columns into a long `Attribute`/`Value` pair.
3. Double-click **Attribute** → rename to `Date`. Double-click **Value** → rename to `Discount`.
4. Change the data type of **Date** to `Date`.
5. Select the **Discount** column → **Transform** tab → **Number column → Standard → Divide…** → enter `100` → **OK**. (The CSV stores discounts as whole numbers like `10` meaning 10%; dividing by 100 gives a usable multiplier.)
6. Rename this query from `Generated-NYC-Taxi-Green-Discounts` to `Discounts`.

#### 3f. Merge Trips + Discounts

1. Select the **Trips** query in Diagram view.
2. **Home → Combine → Merge queries → Merge queries as new**.
3. In the Merge dialog:
   - **Join kind:** `Left outer`
   - **Right table:** `Discounts`
   - Map **`vendorID` ↔ `VendorID`** and **`lpepPickupDatetime` ↔ `Date`** (use the light-bulb icon for suggested mappings).
   - Select **OK**.
4. If you get a **Privacy Levels** warning, select **Continue**, then **Ignore Privacy Levels checks for this document** (OK for this public lab data only).
5. In the new `Merge` query, click the expand arrows on the **Discounts** column header, uncheck everything **except** `Discount`, uncheck **Use original column name as prefix**, and select **OK**.

#### 3g. Add a calculated column and round it

1. **Add column** tab → **Custom column**.
2. Configure:
   - **New column name:** `TotalAfterDiscount`
   - **Data type:** `Currency`
   - **Formula:** `if [totalAmount] > 0 then [totalAmount] * (1 - [Discount]) else [totalAmount]`
3. Select **OK**.
4. With the new column selected: **Transform → Rounding → Round…** → enter `2` → **OK**.
5. In **Query settings**, rename the merge query from `Merge` to `Output`.

#### 3h. Send the output to the Warehouse

1. With the `Output` query selected, on the **Home** tab select **Add data destination → Warehouse**.
2. In **Connect to data destination**, confirm the connection and select **Next**.
3. In **Choose destination target**:
   - Browse to your `WH_GreenCab` warehouse.
   - **Table name:** `nyc_taxi_revenue`
   - Select **Next**.
4. In **Choose destination settings**, leave the **Replace** update method, verify mappings, and select **Save settings**.
5. Back in the editor, select **Save & run** (bottom right, or on the **Home** tab: **Publish**).
6. The dataflow publishes and runs once. Wait for the refresh to complete (watch the spinner on the workspace item list).

> ✅ **Checkpoint:** Open `WH_GreenCab` → Explorer → `dbo.nyc_taxi_revenue` should now exist, and a `SELECT TOP 100 *` should return trips with a `Discount` and `TotalAfterDiscount` column populated.

> ⚠️ **Expected artifact:** When your first Dataflow Gen2 runs in a workspace, Fabric provisions hidden `DataflowsStaging…` items. Don't delete them — they're required.

---

### Task 4 — Orchestrate and schedule with a pipeline (15 min)

Now you wrap the two moving parts (Copy job logic + Dataflow) in a single pipeline so the whole thing refreshes on a daily schedule.

1. In the workspace, select **+ New item → Data pipeline**. Name it `PL_Daily_Taxi_Revenue_Refresh` and select **Create**.
2. On the canvas, select **Pipeline activity → Copy data → Add to canvas** (or use the **Copy data assistant** from the Home ribbon).
3. Configure the Copy activity exactly like Task 2: source = **Sample data → NYC Taxi – Green**, destination = `WH_GreenCab.dbo.bronze_nyc_taxi_green`, mode = **Overwrite**. Rename the activity to `Copy NYC Taxi Bronze`.
4. Drag a second activity onto the canvas: **Activities → Dataflow**. Rename it to `Transform and Load Revenue`.
5. In the Dataflow activity's **Settings** tab, pick `DF_Transform_Taxi_Revenue` from the dropdown.
6. **Chain them:** drag the green "On success" arrow from the Copy activity to the Dataflow activity. The Dataflow will only run if the Copy succeeds.
7. Select **Save** (ribbon). Then select **Run** to validate end-to-end. Watch the **Output** pane until both activities show **Succeeded**.

#### Attach the daily schedule

1. On the **Home** ribbon of the pipeline, select **Schedule → Add schedule**.
2. Configure:
   - **Scheduled run:** On
   - **Schedule type:** Fixed schedule
   - **Repeat:** Daily
   - **Time:** 06:00
   - **Time zone:** your local time zone
   - **Start date:** today
   - **End date:** something far in the future, e.g. `2099-01-01` (Fabric requires an end date — there is no open-ended option).
3. Select **Apply / Save**.

> ✅ **Checkpoint:** Back in the workspace, the pipeline should show a "Scheduled" marker. Selecting **Schedule → View schedules** should list one daily schedule.

---

### Task 5 — Verify and monitor (5 min)

1. Open `WH_GreenCab` → **New SQL query** and run:

   ```sql
   SELECT TOP (20)
       vendorID,
       lpepPickupDatetime,
       passengerCount,
       totalAmount,
       Discount,
       TotalAfterDiscount
   FROM dbo.nyc_taxi_revenue
   ORDER BY lpepPickupDatetime DESC;
   ```

   You should see 20 rows, all with `passengerCount > 0`, all in January 2015, with a `Discount` between 0 and 1 and a non-null `TotalAfterDiscount`.

2. Open the workspace **Monitoring hub** (left-nav **Monitor** icon):
   - Filter to **Pipeline runs** — you should see `PL_Daily_Taxi_Revenue_Refresh` with status **Succeeded**.
   - Filter to **Dataflow Gen2 refreshes** — you should see `DF_Transform_Taxi_Revenue` with a recent success.
   - Click into a run and note the per-activity duration. This is the data you use to troubleshoot failed or slow runs on the exam and in real life.

---

## 7. Acceptance criteria

A student has completed the lab when **all** of the following are true:

- [ ] `WH_GreenCab` contains a `bronze_nyc_taxi_green` table with > 0 rows.
- [ ] `WH_GreenCab` contains an `nyc_taxi_revenue` table with a `TotalAfterDiscount` column.
- [ ] Running `SELECT COUNT(*) FROM dbo.nyc_taxi_revenue` returns > 0.
- [ ] Pipeline `PL_Daily_Taxi_Revenue_Refresh` has at least one **Succeeded** run in Monitor.
- [ ] Pipeline `PL_Daily_Taxi_Revenue_Refresh` has an active daily fixed schedule.

---

## 8. Stretch goals (if you finish early)

Pick any of these. They are all fair game as DP-700 questions:

1. **Incremental instead of full load.** Change the Copy activity from Overwrite to Append, and add a pipeline-level `Delete` activity or a `Script` activity (`TRUNCATE TABLE`) to keep the pipeline idempotent. Discuss the trade-offs with the instructor.
2. **Replace Replace with Append in the Dataflow destination.** What happens to duplicates? How would you fix it with a SQL `MERGE` step in the Warehouse afterwards?
3. **Add a failure path.** Drag a **Teams** or **Outlook** activity (if you have that connector) from the *On fail* output of the Dataflow activity so the team is alerted.
4. **Convert the date filter to a pipeline parameter.** Change the dataflow to accept `StartDate` and `EndDate` and pass them from pipeline parameters (public parameters mode).
5. **Swap CSV for Excel.** Upload the discounts file to OneLake as an `.xlsx`, change the Dataflow source to Excel, and re-run. Confirm outputs are identical.

---

## 9. Cleanup

If this workspace is shared/reused, at the end of the lab:

1. Disable the pipeline schedule (so it stops consuming CU).
2. Drop the Warehouse tables if you don't need them: `DROP TABLE dbo.bronze_nyc_taxi_green; DROP TABLE dbo.nyc_taxi_revenue;`
3. Delete the three items (`CJ_Load_NYC_Taxi_Green`, `DF_Transform_Taxi_Revenue`, `PL_Daily_Taxi_Revenue_Refresh`) or the whole workspace.

---

## 10. Instructor notes

- **Why Dataflow Gen2, not a notebook or T-SQL?** This is a DP-700 talking point. The transformations here are low-code, involve a non-tabular source (CSV over HTTPS), and suit a citizen-developer audience. Notebooks (PySpark) would be overkill; T-SQL can't easily pull an external HTTPS CSV without a stored procedure + external data source setup. Make sure students can justify this choice out loud.
- **Why two sources?** The CSV + sample combination exercises the "mash-up" pattern Power Query was designed for, and forces the student to resolve a **privacy levels** prompt — a common source of production failures.
- **Capacity note:** The Dataflow Gen2 run and pipeline run both consume Fabric CU. On a Trial capacity, 60 students running this concurrently will push smoothing boundaries. For large cohorts, stagger starts or split across capacities.
- **Known gotchas:**
  - Students sometimes forget to untick **Use original column name as prefix** when expanding the merged `Discounts` column — you end up with `Discounts.Discount` instead of `Discount`, which breaks the custom-column formula.
  - The **Replace** update method on the Warehouse destination rewrites the whole table on every refresh — fine for this lab, wrong for terabyte-scale; flag it.
  - The CSV encodes discounts as `10`, `15`, etc., meaning "10%", "15%". If students forget the Divide-by-100 step, `TotalAfterDiscount` will go negative. Use this as a teaching moment about validating source data assumptions.

---

*End of lab.*
