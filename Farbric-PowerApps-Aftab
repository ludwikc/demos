# Fabric Demo: Shoe Price History with Current & Previous Price

🧠 This repo contains a minimal **Fabric Warehouse + Power Apps** demo that tracks shoe prices over time and exposes **current** and **previous** price for each product.

---

## 1. Scenario Overview

We manage a catalog of shoes. Business wants:

- Every price change to be **stored as a new row** (immutable history).
- Ability to view:
  - **Current price** (latest by time)
  - **Previous price** (second latest)
- All data lives in **Microsoft Fabric Warehouse** (Premium capacity).
- Power Apps writes price changes, Fabric/Power BI reads the history.

The pattern is essentially a **tiny SCD Type 2** dimension for prices.

---

## 2. Objects Overview

This demo uses:

1. `dbo.Shoes` – master data for shoes.
2. `dbo.ShoePriceHistory` – immutable price history with `EffectiveFrom` / `EffectiveTo`.
3. `dbo.vShoeCurrentPreviousPrice` – view that exposes current & previous price per shoe.

---

## 3. Core T-SQL Scripts

Run the scripts below in your **Fabric Warehouse SQL endpoint**.

> Adjust schema names or data types as needed for your environment.

### 3.1. Drop existing objects (optional, for repeatable demo)

```sql
-- Drop view if it already exists
IF OBJECT_ID('dbo.vShoeCurrentPreviousPrice', 'V') IS NOT NULL
    DROP VIEW dbo.vShoeCurrentPreviousPrice;
GO

-- Drop history table if it already exists
IF OBJECT_ID('dbo.ShoePriceHistory', 'U') IS NOT NULL
    DROP TABLE dbo.ShoePriceHistory;
GO

-- Drop master table if it already exists
IF OBJECT_ID('dbo.Shoes', 'U') IS NOT NULL
    DROP TABLE dbo.Shoes;
GO

```
⸻

3.2. Create dbo.Shoes (master data)
```sql
CREATE TABLE dbo.Shoes
(
    ShoeId      INT IDENTITY(1,1) NOT NULL,
    SkuCode     NVARCHAR(50)      NOT NULL,
    ShoeName    NVARCHAR(200)     NOT NULL,
    Brand       NVARCHAR(100)     NULL,
    Category    NVARCHAR(100)     NULL,

    CONSTRAINT PK_Shoes PRIMARY KEY (ShoeId)
);
GO
```

⸻

3.3. Create dbo.ShoePriceHistory (immutable price history)

🧠 Each row represents a price version for a given ShoeId.
	•	EffectiveFrom: when the price started to be valid.
	•	EffectiveTo:
	•	NULL → current version
	•	non-NULL → historical (closed) version
```sql
CREATE TABLE dbo.ShoePriceHistory
(
    PriceId         INT IDENTITY(1,1) NOT NULL,
    ShoeId          INT               NOT NULL,
    Price           DECIMAL(18, 2)    NOT NULL,

    EffectiveFrom   DATETIME2(3)      NOT NULL
        CONSTRAINT DF_ShoePriceHistory_EffectiveFrom
        DEFAULT SYSUTCDATETIME(),

    EffectiveTo     DATETIME2(3)      NULL,

    ChangedOn       DATETIME2(3)      NOT NULL
        CONSTRAINT DF_ShoePriceHistory_ChangedOn
        DEFAULT SYSUTCDATETIME(),

    ChangedBy       NVARCHAR(256)     NULL,

    CONSTRAINT PK_ShoePriceHistory PRIMARY KEY (PriceId),

    CONSTRAINT FK_ShoePriceHistory_Shoes
        FOREIGN KEY (ShoeId)
        REFERENCES dbo.Shoes (ShoeId)
);
GO
```
3.3.1. Indexes (recommended for performance)
🧠 For typical queries, you’ll filter by ShoeId and sort by EffectiveFrom desc.
```sql
CREATE INDEX IX_ShoePriceHistory_Shoe_EffectiveFrom
ON dbo.ShoePriceHistory (ShoeId, EffectiveFrom DESC);
GO
```

⸻

3.4. Create view dbo.vShoeCurrentPreviousPrice

🧠 This view uses a window function and simple aggregation to expose:
	•	CurrentPrice
	•	PreviousPrice
	•	CurrentEffectiveFrom
	•	PreviousEffectiveFrom

for each ShoeId.
```sql
CREATE VIEW dbo.vShoeCurrentPreviousPrice
AS
WITH Ranked AS
(
    SELECT
        ShoeId,
        Price,
        EffectiveFrom,
        ROW_NUMBER() OVER
        (
            PARTITION BY ShoeId
            ORDER BY EffectiveFrom DESC
        ) AS rn
    FROM dbo.ShoePriceHistory
)
SELECT
    ShoeId,
    MAX(CASE WHEN rn = 1 THEN Price         END) AS CurrentPrice,
    MAX(CASE WHEN rn = 2 THEN Price         END) AS PreviousPrice,
    MAX(CASE WHEN rn = 1 THEN EffectiveFrom END) AS CurrentEffectiveFrom,
    MAX(CASE WHEN rn = 2 THEN EffectiveFrom END) AS PreviousEffectiveFrom
FROM Ranked
GROUP BY
    ShoeId;
GO
```
In Power Apps / Power BI you can now simply query dbo.vShoeCurrentPreviousPrice to get current & previous price per shoe without re-implementing the logic.

⸻

4. Sample Seed Data (for Demo)

🧠 Use this to quickly get some data into your demo before wiring up Power Apps.

4.1. Insert sample shoes
```sql
INSERT INTO dbo.Shoes (SkuCode, ShoeName, Brand, Category)
VALUES
    ('SH-0001', N'Runner X',     N'Contoso Sports', N'Running'),
    ('SH-0002', N'Street Flex',  N'Fabrikam Urban', N'Sneakers'),
    ('SH-0003', N'Mountain Pro', N'Adventure Co',   N'Hiking');
GO
```
4.2. Insert sample price history

🧠 Example: Shoe SH-0001 has had three price versions over time.
```sql
-- Shoe 1: 3 price changes (latest has EffectiveTo = NULL)
INSERT INTO dbo.ShoePriceHistory (ShoeId, Price, EffectiveFrom, EffectiveTo, ChangedOn, ChangedBy)
VALUES
    -- Oldest price
    (1, 79.99,  '2025-01-01T08:00:00', '2025-03-01T09:00:00', '2025-01-01T08:00:00', N'demo@contoso.com'),
    -- Previous price
    (1, 89.99,  '2025-03-01T09:00:00', '2025-06-01T10:00:00', '2025-03-01T09:00:00', N'demo@contoso.com'),
    -- Current price
    (1, 99.99,  '2025-06-01T10:00:00', NULL,                  '2025-06-01T10:00:00', N'demo@contoso.com');

-- Shoe 2: 2 price changes
INSERT INTO dbo.ShoePriceHistory (ShoeId, Price, EffectiveFrom, EffectiveTo, ChangedOn, ChangedBy)
VALUES
    (2, 59.99,  '2025-02-01T10:00:00', '2025-05-01T09:00:00', '2025-02-01T10:00:00', N'demo@contoso.com'),
    (2, 69.99,  '2025-05-01T09:00:00', NULL,                  '2025-05-01T09:00:00', N'demo@contoso.com');

-- Shoe 3: 1 price only (no "previous" yet)
INSERT INTO dbo.ShoePriceHistory (ShoeId, Price, EffectiveFrom, EffectiveTo, ChangedOn, ChangedBy)
VALUES
    (3, 109.99, '2025-04-15T11:00:00', NULL,                  '2025-04-15T11:00:00', N'demo@contoso.com');
GO
```
You can now test the view:
```sql
SELECT
    s.SkuCode,
    s.ShoeName,
    v.CurrentPrice,
    v.PreviousPrice,
    v.CurrentEffectiveFrom,
    v.PreviousEffectiveFrom
FROM dbo.Shoes s
LEFT JOIN dbo.vShoeCurrentPreviousPrice v
    ON s.ShoeId = v.ShoeId
ORDER BY s.ShoeId;
GO
```

⸻

5. How Power Apps Should Interact with This Model (Conceptual)

🧠 Typical update pattern from Power Apps:
	1.	Find current row for a shoe:
	•	ShoeId = selected shoe
	•	EffectiveTo IS NULL
	2.	Close that row: set EffectiveTo = Now() / UtcNow() (from Power Apps).
	3.	Insert a new row:
	•	ShoeId = selected shoe
	•	Price = NewPrice (from user input)
	•	EffectiveFrom = Now() / UtcNow()
	•	EffectiveTo = NULL
	•	ChangedBy = User().Email or similar

All price history is now immutable, and your vShoeCurrentPreviousPrice view updates automatically.

⸻

6. How Fabric / Power BI Uses This Model

🧠 In Fabric, build a semantic model (dataset) that includes:
	•	dbo.Shoes (dimension)
	•	dbo.ShoePriceHistory (fact/history), or directly dbo.vShoeCurrentPreviousPrice for quick measures.

Example Power BI visuals:
	•	Line chart:
	•	Axis: EffectiveFrom
	•	Values: Price
	•	Filter: selected ShoeId.
	•	Cards:
	•	CurrentPrice from vShoeCurrentPreviousPrice
	•	PreviousPrice from vShoeCurrentPreviousPrice

This demonstrates a clean story:

Power Apps writes versioned prices into Fabric Warehouse → Fabric exposes current/previous prices via a view → Power BI and other tools reuse the same central logic.

⸻

7. Files and Structure Suggestion for GitHub

🧠 Suggested repo structure:

fabric-shoe-price-demo/
├─ README.md          # This markdown file
├─ sql/
│  ├─ 01-create-tables.sql
│  ├─ 02-create-view.sql
│  └─ 03-seed-data.sql
└─ powerapps/
   └─ notes-powerapp-implementation.md  # optional, screenshots & formulas



