# E-Commerce Analytics Platform
End-to-end analytics project analyzing customer behavior, product performance, and experimentation frameworks using DuckDB, dbt, and Python.

## 🎯 Project Overview
This project demonstrates complete data analytics capabilities by building an analytics platform from raw e-commerce data. Includes customer segmentation, cohort analysis, product performance tracking, and A/B testing frameworks.

**Key capabilities showcased:**
- Advanced SQL \& dimensional data modeling (dbt)
- Data pipeline development with DuckDB
- Statistical analysis \& experimentation
- Business intelligence \& visualization

## 🛠️ Tech Stack
- **Database:** DuckDB
- **Transformation:** dbt
- **Analysis:** Python (pandas, scipy, numpy)
- **Visualization:** Tableau Public
- **Version Control:** Git

## 📊 Key Features
### Customer Analytics
- RFM segmentation analysis
- Customer lifetime value modeling
- Cohort retention tracking
  
### Product Analytics
- Product performance metrics
- Reorder rate analysis
- Market penetration tracking

### Experimentation
- Bayesian A/B testing framework
- Statistical significance testing
- Sample size calculators

## 🚀 Getting Started

### Prerequisites
```bash

Python 3.8+

```
### Installation
**1. Clone the repository**
```bash

git clone https://github.com/parthchib/ecommerce-analytics-platform.git

cd ecommerce-analytics-platform

```
**2. Create virtual environment**
```bash

python -m venv venv

source venv/bin/activate  # Mac/Linux

```
**3. Install dependencies**
```bash

pip install -r requirements.txt

```
**4. Download dataset**
- Visit \[Instacart Market Basket Analysis](https://www.kaggle.com/c/instacart-market-basket-analysis/data)
- Download all CSVs
- Place in `raw\_data/` folder

**5. Load data**
```bash

python data/load\_data.py

```
## 📁 Project Structureecommerce-analytics-platform/

├── data/              # Data loading scripts
├── raw\_data/          # Raw CSV files (not tracked)
├── dbt/               # dbt models \& transformations
├── analysis/          # Jupyter notebooks
├── sql/               # Advanced SQL examples
├── docs/              # Documentation
└── README.md

## 📈 Results
**(Will be updated with dashboards and insights)**

## 🧪 A/B Testing Framework
**(Coming soon)**

## 📝 Methodology
**(Will document approach and key decisions)**

## 🤝 Contact
**Parth Chib**
- LinkedIn: \[linkedin.com/in/parthchib31](https://linkedin.com/in/parthchib31)
- Email: parthchib27@gmail.com

_This project was built as a personal learning initiative to deepen expertise in data analytics and experimentation frameworks._

