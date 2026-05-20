# NYC Taxi Project

PySpark project for cleaning, transforming, and analyzing **NYC Yellow Taxi Trip Records**.

## Project Goal

This project processes large-scale NYC taxi trip data using **PySpark**.

The pipeline includes:

- data cleaning
- feature engineering
- exploratory data analysis (EDA)
- trip and revenue analysis

---

## Tech Stack

- Python
- PySpark
- uv
- Ruff
- Prek
- Git / GitHub

---

## Project Structure

```text
.
├── analysis.py
├── cleaning.py
├── main.py
├── exploration.md
├── data/
├── pyproject.toml
├── uv.lock
└── .pre-commit-config.yaml
```

---

## Installation

Clone repository:

```bash
git clone https://github.com/AnastasiiaHorobets/project.git
cd project
```

Install dependencies:

```bash
uv sync
```

---

## Run Project

Run the pipeline:

```bash
uv run python main.py
```

---

## Code Quality

Run linting:

```bash
uvx ruff check .
```

Run formatting and hooks:

```bash
prek run --all-files
```

---

## Exploratory Analysis

Dataset exploration notes are available in:

```text
exploration.md
```