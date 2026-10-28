# Lakeflow Declarative Pipelines: Beginner Intro

## Part 1: Theory

### The problem

In normal PySpark, you manage every step yourself:

- Which table runs first
- Where to save the results
- How to pick up only new data

This gets messy as projects grow.

### The idea

Think of ordering food at a restaurant.

- **Imperative (normal PySpark):** you cook and handle every step.
- **Declarative (Lakeflow):** you say what you want, and the kitchen handles the steps.

You describe the tables you want ("table B is built from table A"). Databricks works out the order, the saving, and the updates.

> Name note: this used to be called **Delta Live Tables (DLT)**. It is now **Lakeflow Spark Declarative Pipelines**. Old tutorials use the old name.

### Three building blocks

| Block | Plain meaning | Python decorator |
| --- | --- | --- |
| Streaming table | A table that keeps picking up new data | `@dp.table` |
| Materialized view | A stored result of a query | `@dp.materialized_view` |
| Temporary view | A temporary step inside the pipeline, not saved | `@dp.temporary_view` |

### `readStream` vs `read`

- `spark.read.table(...)` reads **all rows every time**.
- `spark.readStream.table(...)` reads **only new rows since the last run**, and remembers where it stopped.

Analogy: a notebook of orders. `read` re-reads the whole notebook. `readStream` uses a bookmark and reads only the new pages.

In this demo:

- `orders_raw` is a streaming table, so it uses `readStream`.
- `sales_by_day` is a materialized view, so it uses plain `read` and recalculates from all the orders.

---

## Part 2: Demo

### Step 1: Create the sample data

Run in a SQL editor or notebook:

```sql
CREATE TABLE workspace.default.orders (
  order_id INT, order_date DATE, amount DOUBLE
);

INSERT INTO workspace.default.orders VALUES
  (1, '2026-10-01', 100),
  (2, '2026-10-01', 250),
  (3, '2026-10-02', 175);
```

Use your own catalog and schema if they differ from `workspace.default`.

### Step 2: Create the pipeline

Jobs & Pipelines → Create → ETL pipeline. Name it `my_first_pipeline` and set the catalog and schema to the ones above.

The pipeline starts with a **Python** file (`my_transformation.py`), so use the Python code below. Do not paste SQL into it.

### Step 3: Paste the pipeline code

```python
from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table
def orders_raw():
    return spark.readStream.table("workspace.default.orders")

@dp.materialized_view
def sales_by_day():
    return (
        spark.read.table("orders_raw")
        .groupBy("order_date")
        .agg(F.sum("amount").alias("total_sales"))
    )
```

Line by line:

- `@dp.table`: build a streaming table from what the function returns.
- `def orders_raw():`: the function name becomes the table name.
- `spark.readStream.table(...)`: read only the new rows.
- `return`: hand the data to Databricks. No manual save or checkpoint needed.

### Step 4: Run the pipeline

Click **Run pipeline**. When it finishes, show the graph: two boxes with an arrow between them.

Say: "I never told it which table runs first. Databricks saw that `sales_by_day` depends on `orders_raw` and worked out the order."

### Step 5: Check the results

| Table | Expected |
| --- | --- |
| `orders_raw` | 3 rows |
| `sales_by_day` | Oct 1 = 350, Oct 2 = 175 |

### Step 6: Add new data and run again

```sql
INSERT INTO workspace.default.orders VALUES
  (4, '2026-10-02', 300),
  (5, '2026-10-03', 120);
```

Run the pipeline again.

| Table | Expected |
| --- | --- |
| `orders_raw` | Picks up only the 2 new rows (5 total) |
| `sales_by_day` | Oct 1 = 350, Oct 2 = 475, Oct 3 = 120 |

---

## Part 3: Recap

- You describe the tables. Databricks handles the order, saving, and updates.
- Streaming table: read only what's new.
- Materialized view: recalculate from everything.

## Good to know

A streaming read expects the source table to only receive new rows. If you update or delete rows in `orders`, the pipeline gives an error. This can be a future video.

## Next video

Streaming table vs materialized view, explained in more detail.