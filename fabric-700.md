# DP-700 Capstone Lab  
## Contoso Mobility – Sales Analytics Platform (Fabric End-to-End)

**Time:** 90–120 minutes  
**Environment:** New, empty Fabric workspace with internet access to Microsoft Learn labs and GitHub

---

## Overview

This capstone uses **exactly the same data and resources** as the official Fabric labs from the repository:

- **Online instructions index:**  
  https://microsoftlearning.github.io/mslearn-fabric/Instructions/Labs/

- **GitHub repo (instructions + assets):**  
  https://github.com/MicrosoftLearning/mslearn-fabric

You'll reuse the **`sales.csv`** dataset and related materials that appear in:

- Lab "Create a Microsoft Fabric Lakehouse" (`01-lakehouse`)  
  https://microsoftlearning.github.io/mslearn-fabric/Instructions/Labs/01-lakehouse.html

- Lab "Load data into a warehouse using T-SQL" (`06a-data-warehouse-load`)  
  https://microsoftlearning.github.io/mslearn-fabric/Instructions/Labs/06a-data-warehouse-load.html

---

## Scenario

You are a Fabric Data & Analytics Engineer working for **Contoso Mobility**.

Your task is to build, from scratch, a small but realistic **end-to-end Fabric solution**:

1. Ingest Contoso's sales transactions from `sales.csv` into a lakehouse (Bronze).
2. Clean and standardize the data into a curated Silver table (using a notebook + Data Wrangler).
3. Load the curated data into a warehouse (Gold) using T-SQL.
4. Build a semantic model on top of the warehouse.
5. Create a Power BI report in Fabric.
6. Monitor and inspect warehouse activity.

All steps are aligned with and explicitly reference the **DP-700 Fabric labs** so everything feels familiar.

---

## Part 1 – Workspace and Item Setup (5–10 minutes)

### Goal

Create a clean workspace and the core items:

- Lakehouse: `ContosoLake`
- Warehouse: `ContosoDWH`

### Steps

1. Open a browser and go to the Fabric home page:  
   `https://app.fabric.microsoft.com/home?experience=fabric`

2. Sign in with the same account you used for the DP-700 labs.

3. In the left navigation, select **Workspaces**.

4. Create a new workspace:
   1. Select **New workspace**.
   2. Name it exactly: `DP700-ContosoMobility-Capstone`
   3. In **Advanced**, choose a mode that includes Fabric capacity (Trial, Premium, or Fabric).
   4. Confirm creation and wait until the workspace opens and appears empty.

5. In this workspace, create the **lakehouse**:
   1. In the left menu, select **Create** (or **+ New item**).
   2. Under **Data Engineering**, choose **Lakehouse**.
   3. Name it: `ContosoLake`.
   4. Leave "Lakehouse schemas (Public Preview)" disabled (as in Lab `01-lakehouse`).
   5. Select **Create** and wait until the lakehouse opens.

6. Back in the workspace, create the **warehouse**:
   1. In the workspace, select **Create**.
   2. Under **Data Warehouse**, choose **Warehouse**.
   3. Name it: `ContosoDWH`.
   4. Select **Create** and wait until the warehouse appears in the workspace.

You should now see **two items** in the workspace:
- Lakehouse: `ContosoLake`
- Warehouse: `ContosoDWH`

---

## Part 2 – Ingest Raw Data into Lakehouse (Bronze) (10–15 minutes)

### Goal

Download the **same Contoso sales dataset** used in labs `01-lakehouse` and `06a-data-warehouse-load` and store it as a raw file in the lakehouse.

### Files Used

1. **`sales.csv`** – downloaded from the dp-data GitHub repo:
   - **URL** (as used in official labs):  
     `https://raw.githubusercontent.com/MicrosoftLearning/dp-data/main/sales.csv`

   This file contains (among others) the columns:
   - `SalesOrderNumber`
   - `SalesOrderLineNumber`
   - `OrderDate`
   - `CustomerName`
   - `Email`
   - `Item`
   - `Quantity`
   - `UnitPrice`
   - `Tax`

2. **Optional helper snippet file** for SQL later (from mslearn-fabric repo):
   - `01-Snippets.txt`  
     `https://github.com/MicrosoftLearning/mslearn-fabric/raw/main/Allfiles/Labs/01/Assets/01-Snippets.txt`

### Steps

1. **Download `sales.csv`:**
   1. Open a **new** browser tab.
   2. Paste the URL:  
      `https://raw.githubusercontent.com/MicrosoftLearning/dp-data/main/sales.csv`
   3. When the raw CSV content appears, use the browser's **Save As**:
      - File name: `sales.csv`
      - File type: CSV (or "Text" but with `.csv` extension)

2. **Ingest into the `ContosoLake` lakehouse:**
   1. Return to the Fabric tab with **ContosoLake** open in **Lakehouse** view.
   2. In the **Explorer** pane, locate the **Files** node.
   3. Use the ellipsis (…) for **Files** → **New subfolder**.
   4. Name the folder: `bronze`.
   5. Select the new `bronze` folder.
   6. In the ellipsis (…) menu for `bronze`, select **Upload** → **Upload files**.
   7. Browse to the downloaded `sales.csv` on your machine.
   8. Upload it into `Files/bronze`.

3. **Verify the bronze layer:**
   1. In the Explorer, expand **Files** → **bronze**.
   2. Confirm that `sales.csv` is visible.
   3. Select `sales.csv` and check the **Preview** panel to confirm the data looks reasonable (header row + sample rows).

**At this point:**
- You have **raw data (Bronze)** stored as a file at `Files/bronze/sales.csv`.

---

## Part 3 – Create a Silver Table with Notebook + Data Wrangler (20–25 minutes)

### Goal

Transform the raw CSV into a cleaned **Silver table** using:

- A **notebook** (PySpark)
- Optional **Data Wrangler** operations (similar to lab "Preprocess data with Data Wrangler in Microsoft Fabric")

You'll create a curated table called `sales_silver` in the lakehouse.

### Steps

#### 3.1 Open a notebook on the bronze folder

1. With `ContosoLake` open and the `Files/bronze` folder selected:
   1. On the toolbar, open the **Open notebook** menu.
   2. Choose **New notebook**.
   3. Wait until the notebook interface appears with a single empty code cell.

2. Rename the notebook:
   1. At the top-left, select the default name (for example "Notebook 1").
   2. Rename it to: `Transform sales to Silver`.

3. Attach the notebook to the lakehouse:
   1. In the notebook's **Lakehouse** pane (usually on the left), ensure `ContosoLake` is attached.
   2. If not attached:
      - Select **Add lakehouse** (or **Add data**).
      - Choose the `ContosoLake` item and attach it.

#### 3.2 Load raw data into a DataFrame

4. In the first code cell, paste and run this PySpark code:

   ```python
   from pyspark.sql import functions as F

   # Load raw Contoso sales data from the bronze folder
   df_raw = (
       spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv("Files/bronze/sales.csv")
   )

   display(df_raw.limit(10))
   ```

5. Wait for the Spark session to start and confirm that:
   - Column names and types look correct.
   - There are no obvious parsing issues.

#### 3.3 Use Data Wrangler on the sales data (optional but recommended)

6. **Open Data Wrangler from the DataFrame:**
   1. In the notebook's Data pane or output area, find the `df_raw` DataFrame.
   2. Use the context menu (… or right-click) and select **Open in Data Wrangler** (similar pattern as in the "Preprocess data with Data Wrangler" lab).

7. **In Data Wrangler, apply common clean-up transformations** (you can pick reasonable operations based on what you see; examples):
   - **Trim whitespace in text columns:**
     - For columns like `CustomerName`, `Email`, `Item`, apply a trim or equivalent text-cleanup operation.
   - **Normalize casing:**
     - For example, capitalize or title-case `CustomerName` and `Item`.
   - **Handle nulls:**
     - Replace missing `Quantity` or `UnitPrice` with 0 or filter out rows you consider invalid.
   - **Validate date:**
     - Confirm `OrderDate` is parsed as a date type.

8. **In Data Wrangler, once you are satisfied:**
   1. Use the **Add code to notebook** option.
   2. This will generate a PySpark transformation function or set of operations.
   3. Confirm that the generated code is inserted into a new code cell in the notebook.

9. **Adjust the generated code** so that:
   - It reads from `df_raw`.
   - It produces a curated DataFrame named `df_silver`.

   **Example pattern** (adapt to the actual code Data Wrangler generated):

   ```python
   # Example only – adapt to the actual Data Wrangler code
   df_silver = (
       df_raw
       .withColumn("CustomerName", F.trim(F.col("CustomerName")))
       .withColumn("Email", F.trim(F.col("Email")))
       .withColumn("Item", F.trim(F.col("Item")))
       .dropDuplicates()
   )

   display(df_silver.limit(10))
   ```

10. Run the transformation cell and verify:
    - Sample rows look correct.
    - The number of rows is plausible.
    - No obviously broken values (e.g., negative quantities) remain.

#### 3.4 Save the curated data as a Silver table

11. In a new code cell, write the DataFrame to a managed table:

    ```python
    # Overwrite or create a managed Silver table in the lakehouse
    (
        df_silver
        .write
        .mode("overwrite")
        .saveAsTable("sales_silver")
    )
    ```

12. After the cell finishes:
    1. In the lakehouse Explorer pane, expand **Tables**.
    2. Confirm that a table named `sales_silver` now exists.
    3. Select it and open a Preview to confirm the data is present.

**You now have:**
- **Bronze:** `Files/bronze/sales.csv` (raw file)
- **Silver:** `sales_silver` (cleaned Delta table in the lakehouse)

---

## Part 4 – Load Gold Data into the Warehouse (15–20 minutes)

### Goal

Use the same pattern as in lab "Load data into a warehouse using T-SQL" (`06a-data-warehouse-load`) to create Sales schema + dimension + fact tables in `ContosoDWH` and load data from the lakehouse's `sales_silver` table.

### Concept

You will:

1. Expose `sales_silver` as a staging table in the lakehouse SQL endpoint (if you prefer, you can create `staging_sales` too).
2. Create Sales schema in the warehouse.
3. Define `Dim_Customer`, `Dim_Item`, and `Fact_Sales`.
4. Create a view `Sales.Staging_Sales` in the warehouse pointing to the lakehouse table.
5. Use a stored procedure to load data from staging into the warehouse tables (pattern taken from the official lab).

### Steps

#### 4.1 (Optional) Create a staging table in the lakehouse

This step replicates the pattern from `06a-data-warehouse-load`, where `staging_sales` is created from `sales.csv`. Here you already have `sales_silver`, so this is optional. If you want to match the official lab exactly:

1. In `ContosoLake`, switch to the **SQL analytics endpoint**.

2. Use **New SQL query** and run something like:

   ```sql
   CREATE TABLE dbo.staging_sales
   AS
   SELECT *
   FROM dbo.sales_silver;
   ```

3. Confirm the table `dbo.staging_sales` appears in the SQL endpoint (Tables node).

#### 4.2 Create warehouse schema and core tables

2. **Open the `ContosoDWH` warehouse:**
   1. From the workspace, select `ContosoDWH`.
   2. Make sure you're in the warehouse view (not SQL endpoint of the lakehouse).

3. **Create the schema and the dimension/fact tables:**
   1. Select **New SQL query** in the warehouse.
   2. Go to the online instructions for lab `06a-data-warehouse-load`:  
      https://microsoftlearning.github.io/mslearn-fabric/Instructions/Labs/06a-data-warehouse-load.html
   3. Scroll to the section **"Create fact table, dimensions and view"**.
   4. Copy the T-SQL script that:
      - Creates schema `[Sales]`
      - Creates tables `Sales.Fact_Sales`, `Sales.Dim_Customer`, `Sales.Dim_Item`  
      (you can paste it as-is into your query window).
   5. Update nothing for now (keep the schema and table definitions aligned with the lab).
   6. Run the query.

4. **In the warehouse Explorer:**
   1. Expand **Schemas** → **Sales** → **Tables**.
   2. Verify that:
      - `Sales.Fact_Sales`
      - `Sales.Dim_Customer`
      - `Sales.Dim_Item`  
      exist.

#### 4.3 Create a view pointing to the lakehouse staging/silver table

5. In the same instructions page `06a-data-warehouse-load`, under the same section, there is an example:

   ```sql
   CREATE VIEW Sales.Staging_Sales AS 
   SELECT * FROM [<your lakehouse name>].[dbo].[staging_sales];
   ```

6. **In your warehouse:**
   1. Open a new SQL query.
   2. Paste the view template from the lab.
   3. Replace the placeholder `<your lakehouse name>` with the name of your lakehouse SQL endpoint (typically `ContosoLake`).
   4. If you kept the optional `staging_sales` table:
      - Leave `[dbo].[staging_sales]` as in the lab.
      - Otherwise, you can point directly to `[dbo].[sales_silver]`.
   5. Run the statement.

7. **Verify:**
   1. In Explorer, expand **Schemas** → **Sales** → **Views**.
   2. Confirm `Sales.Staging_Sales` exists.

#### 4.4 Use stored procedure to load data

8. In `06a-data-warehouse-load`, scroll to the section **"Load data to the warehouse"**.
   - It contains a stored procedure `Sales.LoadDataFromStaging (@OrderYear INT)` that:
     - Populates `Sales.Dim_Customer`
     - Populates `Sales.Dim_Item`
     - Loads `Sales.Fact_Sales`

9. **In your warehouse:**
   1. Open a new SQL query.
   2. Copy the entire `CREATE OR ALTER PROCEDURE Sales.LoadDataFromStaging (@OrderYear INT) AS ...` script from the instructions page.
   3. Paste into the query editor and run it (no changes required if your staging view and column names match the lab).

10. **Still in the warehouse, execute the procedure for a chosen year:**
    1. Open a new SQL query (or reuse the same editor).
    2. Run the execution command from the lab, e.g.:

       ```sql
       EXEC Sales.LoadDataFromStaging 2021;
       ```

    3. Wait until it completes successfully.

11. **Validate that data was loaded:**
    1. In Explorer, right-click `Sales.Fact_Sales` → **Preview**.
    2. Confirm that rows are present.
    3. Do the same for `Sales.Dim_Customer` and `Sales.Dim_Item`.

**You now have a Gold layer in the warehouse:**
- **Schema:** Sales
- **Fact table:** Fact_Sales
- **Dimension tables:** Dim_Customer, Dim_Item

---

## Part 5 – Build a Semantic Model on the Warehouse (10–15 minutes)

### Goal

Create a semantic model directly on top of `ContosoDWH`, using the Sales schema tables (similar to the semantic model steps in labs around lakehouses and medallion architectures).

### Steps

1. From the workspace, open the `ContosoDWH` warehouse (if not already open).

2. **Create a new semantic model:**
   1. On the warehouse toolbar, select **New semantic model**.
   2. In the dialog that appears, select these tables:
      - `Sales.Fact_Sales`
      - `Sales.Dim_Customer`
      - `Sales.Dim_Item`
   3. Name the semantic model: `ContosoMobility_SalesModel`.
   4. Confirm creation.

3. **In the semantic model designer:**
   1. Go to the **Model** or **Diagram** view.
   2. Create relationships:
      - `Sales.Dim_Customer[CustomerID]` → `Sales.Fact_Sales[CustomerID]`
      - `Sales.Dim_Item[ItemID]` → `Sales.Fact_Sales[ItemID]`
   3. Ensure the cardinality is Many-to-One from fact to dimension, with a single-direction filter from dimensions to fact.

4. **Create core measures:**
   1. Select `Sales.Fact_Sales` as the active table.
   2. Add a new measure:

      ```dax
      Total Sales Amount :=
          SUMX(
              Sales.Fact_Sales,
              Sales.Fact_Sales[UnitPrice] * Sales.Fact_Sales[Quantity]
          )
      ```

   3. Optional additional measures:
      - **Total Quantity:**

        ```dax
        Total Quantity :=
            SUM ( Sales.Fact_Sales[Quantity] )
        ```

      - **Average Order Value:**

        ```dax
        Average Order Value :=
            DIVIDE (
                [Total Sales Amount],
                DISTINCTCOUNT ( Sales.Fact_Sales[SalesOrderNumber] )
            )
        ```

5. Save the semantic model.

**You now have a reusable semantic layer against the warehouse.**

---

## Part 6 – Build a Power BI Report in Fabric (10–20 minutes)

### Goal

Use the semantic model `ContosoMobility_SalesModel` to build a small but meaningful report inside Fabric.

### Steps

1. In the workspace, find the semantic model `ContosoMobility_SalesModel`.

2. **Create a report:**
   1. Select the semantic model.
   2. Use the ellipsis (…) → **New report** (or equivalent action).
   3. A Power BI report canvas opens, already connected to your semantic model.

3. **On the report canvas, create visuals:**

   1. **Total Sales card:**
      - Add a **Card** visual.
      - Field: `[Total Sales Amount]` measure.
      - Format as currency.

   2. **Top customers bar chart:**
      - Add a **Clustered bar chart**.
      - Axis: `Sales.Dim_Customer[CustomerName]`.
      - Values: `[Total Sales Amount]`.
      - Sort by `[Total Sales Amount]` descending.
      - Optionally, use a Top N filter (e.g., Top 10).

   3. **Top items bar chart:**
      - Add a second bar chart.
      - Axis: `Sales.Dim_Item[ItemName]`.
      - Values: `[Total Sales Amount]`.

   4. **Summary table:**
      - Add a **Table** visual.
      - Columns:
        - `Sales.Dim_Customer[CustomerName]`
        - `[Total Sales Amount]`
        - `[Total Quantity]`
        - `[Average Order Value]`

4. Adjust layout and formatting to make the report readable (titles, labels, number formats).

5. Save the report as: `ContosoMobility-SalesReport`.

**At this point, you can slice and explore sales by customer and item.**

---

## Part 7 – Monitor and Inspect Warehouse Activity (5–10 minutes)

### Goal

Use Fabric's built-in monitoring experience for warehouses to inspect queries and performance (as in the "Monitor a data warehouse in Microsoft Fabric" module).

### Steps

1. Open `ContosoDWH` in the workspace.

2. On the warehouse toolbar, locate and open the **Monitoring** or **View monitoring** option (naming may vary slightly depending on UI updates).

3. **Explore the available views:**
   - **Query history** – list of executed queries.
   - **Query details/performance** – timing and resource usage per query.

4. **Generate some activity:**
   1. Open a **New SQL query** in the warehouse.
   2. Run a query that joins fact and dimensions, for example:

      ```sql
      SELECT TOP 100
          c.CustomerName,
          i.ItemName,
          s.OrderDate,
          s.Quantity,
          s.UnitPrice,
          s.UnitPrice * s.Quantity AS LineAmount
      FROM Sales.Fact_Sales AS s
      JOIN Sales.Dim_Customer AS c
          ON s.CustomerID = c.CustomerID
      JOIN Sales.Dim_Item AS i
          ON s.ItemID = i.ItemID
      ORDER BY LineAmount DESC;
      ```

   3. Run the query and inspect the results.

5. **Return to the Monitoring page:**
   1. Refresh the view.
   2. Locate your recent query.
   3. Inspect:
      - Duration
      - Data processed / IO (where available)
      - Any performance metrics visible.

**You have now closed the loop: from ingestion and modeling all the way to performance observation.**

---

## Final Capstone Outcome

By completing this capstone, students have:

- ✅ Created a dedicated Fabric workspace and items (`ContosoLake`, `ContosoDWH`).
- ✅ Ingested `sales.csv` from the same GitHub source used in official DP-700 labs:  
  `https://raw.githubusercontent.com/MicrosoftLearning/dp-data/main/sales.csv`
- ✅ Implemented a simple Medallion architecture:
  - **Bronze:** file at `Files/bronze/sales.csv`
  - **Silver:** cleaned lakehouse table `sales_silver`
  - **Gold:** warehouse schema `Sales` with `Fact_Sales`, `Dim_Customer`, `Dim_Item`
- ✅ Reused patterns and scripts from the official mslearn-fabric instructions:
  - `01-lakehouse` (lakehouse basics, sales.csv, and 01-Snippets.txt)
  - `06a-data-warehouse-load` (T-SQL loading pattern into the warehouse)
  - Data Wrangler lab for transformation patterns
- ✅ Built a semantic model directly on the warehouse.
- ✅ Produced a Power BI report in Fabric based on that semantic model.
- ✅ Used warehouse monitoring to review and reason about performance.

**This capstone can be run start-to-finish in about 90 minutes in a clean environment, while still feeling tightly integrated with the DP-700 Fabric lab ecosystem you're teaching from.**

---

## Resources

- [Microsoft Learn Fabric Labs](https://microsoftlearning.github.io/mslearn-fabric/Instructions/Labs/)
- [GitHub Repository](https://github.com/MicrosoftLearning/mslearn-fabric)
- [Lab 01: Create a Lakehouse](https://microsoftlearning.github.io/mslearn-fabric/Instructions/Labs/01-lakehouse.html)
- [Lab 06a: Load Data into Warehouse](https://microsoftlearning.github.io/mslearn-fabric/Instructions/Labs/06a-data-warehouse-load.html)
- [Data Wrangler Lab](https://microsoftlearning.github.io/mslearn-fabric/Instructions/Labs/08b-data-science-preprocess-data-wrangler.html)
