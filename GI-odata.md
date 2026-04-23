Here is an end-to-end, 60-minute lab scenario tailored for DP-700 (Microsoft Fabric Data Engineering) students. This exercise is structured to be delivered directly to students, focusing on practical execution and system-level understanding of Fabric's Data Factory and Synapse Data Warehouse components. 

### Lab Objective
Extract data from an external OData service and a local CSV file, transform and merge the data using Dataflow Gen2, load the curated dataset into a Synapse Data Warehouse, and orchestrate the process using a scheduled Data Pipeline.

**Estimated Time:** 60 Minutes
**Prerequisites:** An active Microsoft Fabric trial capacity and an empty Workspace.

---

### Phase 1: Environment Setup (10 Minutes)
**Goal:** Prepare the Fabric workspace and the target Data Warehouse.

1. **Create the Workspace:**
   * Navigate to the Fabric portal (app.fabric.microsoft.com).
   * Click **Workspaces** > **New workspace**.
   * Name it `DP700-[YourInitials]-Workspace` and ensure it is assigned to a Fabric capacity (Trial or Premium).
2. **Create the Synapse Data Warehouse:**
   * Switch to the **Data Warehouse** experience using the persona switcher at the bottom left.
   * Click **Warehouse** to create a new artifact. Name it `SalesDW`.
   * Keep the tab open; the warehouse is currently empty.

### Phase 2: Data Source Preparation (5 Minutes)
**Goal:** Stage the two required data sources.

1. **Source 1 (OData Feed):**
   * We will use the public Northwind OData feed. No setup is required. 
   * URL: `https://services.odata.org/V4/Northwind/Northwind.svc/`
2. **Source 2 (CSV File):**
   * Open Notepad or Excel and create a simple CSV file named `ShippingDiscounts.csv` with the following data:
     ```csv
     ShipRegion,DiscountMultiplier
     RJ,0.90
     NM,0.85
     SP,0.95
     Isle of Wight,0.80
     ```
   * Save this file locally to your machine.

### Phase 3: Extraction and Transformation in Dataflow Gen2 (25 Minutes)
**Goal:** Ingest both sources, clean, merge, and apply business logic.

1. **Initialize Dataflow Gen2:**
   * Switch to the **Data Factory** experience.
   * Click **Dataflow Gen2**. 
   * Rename the dataflow from *Dataflow 1* to `TransformSalesData` (top left corner).
2. **Ingest the OData Feed:**
   * Click **Get Data** > **OData**.
   * Paste the URL: `https://services.odata.org/V4/Northwind/Northwind.svc/`. 
   * Set authentication to Anonymous and click Next.
   * Select the **Orders** table and click **Create**.
3. **Ingest the CSV File:**
   * Click **Get Data** > **Text/CSV**.
   * Choose **Upload file** and browse for the `ShippingDiscounts.csv` you created.
   * Click Next, ensure the columns parse correctly, and click **Create**.
4. **Transform and Merge:**
   * Select the `Orders` query. 
   * In the Home ribbon, click **Merge queries**.
   * Select `Orders` as the top table, and `ShippingDiscounts` as the bottom table.
   * Highlight the `ShipRegion` column in both tables to define the join condition. Set the Join Kind to **Left Outer**. Click OK.
   * In the `Orders` table, expand the newly created merged column and select *only* the `DiscountMultiplier` column.
5. **Add Business Logic (Calculated Column):**
   * Replace null multipliers: Right-click the `DiscountMultiplier` column, select **Replace Values**. Replace `null` with `1.0`.
   * Go to the **Add Column** ribbon > **Custom Column**.
   * Name it `AdjustedFreight`.
   * Formula: `[Freight] * [DiscountMultiplier]`. Ensure the data type is set to Decimal Number.
6. **Clean Up:**
   * Remove unnecessary columns (e.g., keeping only `OrderID`, `CustomerID`, `OrderDate`, `ShipRegion`, `Freight`, and `AdjustedFreight`).

### Phase 4: Loading to Synapse Data Warehouse (10 Minutes)
**Goal:** Configure the output sink for the transformed data.

1. **Set the Destination:**
   * With the `Orders` query selected, look at the bottom right corner in the **Query settings** pane.
   * Under **Data destination**, click the **+** icon and select **Warehouse**.
2. **Configure the Connection:**
   * Sign in with your organizational account if prompted.
   * Navigate through the workspace tree and select `SalesDW`.
   * Choose **New table** and name it `FactOrders`.
   * Click Next.
3. **Map Columns and Publish:**
   * Review the column mapping (Power Query maps identically named columns automatically).
   * Ensure the update method is set to **Replace** (drops and recreates data on each run for this lab).
   * Click **Save settings**.
   * Click **Publish** (bottom right). The dataflow will now save and execute its first run. Wait for the spinning status indicator in the workspace to complete.

### Phase 5: Orchestration and Scheduling (10 Minutes)
**Goal:** Automate the execution using a Data Pipeline.

1. **Create the Pipeline:**
   * In the Data Factory experience, click **Data pipeline**.
   * Name it `DailySalesIngestion`.
2. **Add the Dataflow Activity:**
   * In the pipeline canvas, click **Add pipeline activity** > **Dataflow**.
   * Name the activity `Run TransformSalesData`.
   * Under the **Settings** tab, select the workspace and choose the `TransformSalesData` dataflow.
3. **Configure the Scheduler:**
   * Click the **Schedule** button on the top ribbon.
   * Turn the schedule **On**.
   * Set it to run **Daily** at a specific time (e.g., 02:00 AM).
   * Define the start and end dates, then click **Apply**.
4. **Test the Pipeline:**
   * Click the **Run** button to trigger a manual execution of the pipeline.
   * Monitor the execution in the **Output** tab at the bottom of the screen.

### Verification
Instruct students to switch back to the **Data Warehouse** experience, open `SalesDW`, and write a simple T-SQL query to verify the data ingestion:
```sql
SELECT TOP 10 OrderID, CustomerID, AdjustedFreight 
FROM FactOrders 
ORDER BY OrderDate DESC;
```
