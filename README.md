# Take-Home Assignment: Numerical Data Conversion and Processing Pipeline

Welcome to this take-home assignment!  
This is designed to evaluate your ability to work with data transformation pipelines, optimize for large datasets, and modularize code into a workflow. The assignment is based on a simplified version of an internal data transformer package.

Please **read this document carefully before starting**.  
If anything is unclear, don’t hesitate to ask questions. Communication is part of the evaluation.

---


## Overview

You are provided with a Python package called `bd_transformer`. It contains a class `Transformer` which supports three main methods:
- `fit()`
- `transform()`
- `inverse_transform()`

The `Transformer` works on pandas DataFrames and converts columns into numerical values suitable for model training. It has two internal components:
- **Converter**: extracts numerical values from a column  
- **Normaliser**: scales numerical values using MinMax scaling

The inverse methods of `Converter` and `Normaliser` return a DataFrame with 3 columns:
- `data`: the reversed data
- `valid`: a boolean showing if the input row was valid
- `error`: error message for invalid rows

Install the package using `pip install -e .`

The goal of this assignment is to:
1. Convert the pandas code to a distributed framework (like Spark)
2. Test the new code on large datasets
3. Build a modular and scalable pipeline using Airflow or a similar orchestrator

---

## Step-by-Step Instructions

Please **complete each step before moving to the next**.  
For each step, document the **challenges you faced and how you solved them**.  
This is important and part of the evaluation.

---

### Step 1: Convert to Spark (or other framework)

- Convert the pandas-based `bd_transformer` code to run using **Apache Spark**  
  (or any other distributed framework — let us know which one you’re using).
- Make sure the new code can handle datasets **larger than your system memory**
- **Verify correctness** by comparing output of new code vs old code on small test data

**Deliverables**:
- Updated `bd_transformer` package using Spark
- A script to compare output of new vs old pipeline

---

### Step 2: Test on Large Dataset

- Use `data_gen.py` or your own script to generate a dataset that is **~20x the size of your machine’s memory**
- Record the time taken for:
  - `fit()`
  - `transform()`
  - `inverse_transform()`
- Collect and plot:
  - **CPU usage**
  - **Memory usage**
  - **Disk I/O**

  (If needed, optimize your data generation to avoid long delays.)

**Deliverables**:
- Dataset generation script (if changed)
- Resource usage graphs (CPU, memory, disk I/O)
- Specs of the machine used (RAM, CPU, etc.)
- Time taken for each step

---

### Step 3: Build a Workflow Pipeline

- Convert the Spark version into an **Airflow pipeline** (or similar workflow tool)
- Modularize the code as needed:
  - For example, each column’s `fit()` can run in parallel
- Structure your DAG to be simple, efficient, and easy to debug

**Deliverables**:
- Updated `bd_transformer` package with Airflow pipeline code
- Instructions on how to run the pipeline
- Design choices and trade-offs

---

## Final Deliverables

Please share the following in a **public GitHub repository**:

- Updated `bd_transformer` package
- Script to compare old and new pipelines for correctness
- Large dataset specs and runtime performance
- Resource usage plots (CPU, memory, disk I/O)
- Step-by-step notes on challenges and how you fixed them
- Airflow (or other) pipeline code

> Do not mention the company name or internal details anywhere in the code or documentation.

---

## Evaluation Criteria

We will review your submission on the following:

| Area | What we look for |
|------|------------------|
| Correctness | Does the code produce correct results? |
| Performance | Can it handle large datasets efficiently? |
| Code Quality | Is the code readable and well-structured? |
| Pipeline Design | How well is the workflow modularized and managed in Airflow? |
| Documentation | Are steps, issues, and fixes clearly explained? |
| Communication | Did you ask questions and communicate clearly? |
| Independence | Did you make sensible assumptions where needed? |

---

## Good to Know

- `run.py` runs the original pandas pipeline
- `data_gen.py` generates sample data
- We recommend starting with reading the `bd_transformer` code in detail
- Keep your updates focused and incremental

---

Good luck and have fun! If anything is unclear, please reach out.