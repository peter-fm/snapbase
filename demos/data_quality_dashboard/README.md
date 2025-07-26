# Data Quality Dashboard Demo

This demo showcases how to use Snapbase with statistical analysis to monitor data quality over time using Jensen-Shannon divergence.

## What it demonstrates

- **Snapshot-based data versioning** with Snapbase
- **Distribution drift detection** using Jensen-Shannon divergence
- **Anomaly detection** in data quality monitoring
- **Interactive dashboard** with Streamlit for data quality visualization

## Quick Start

1. **Run the demo**:
```bash
   cd ../demo/data_quality_dashboard/
   uv sync
   uv run streamlit run data_quality_demo.py
```

3. **View the dashboard**: Open your browser to `http://localhost:8501`

## How it works

### Data Generation
- Creates 8 synthetic snapshots of customer data
- 7 snapshots have similar, gradually drifting distributions  
- 1 snapshot (#6) has a significantly different distribution (the anomaly)
- Tracks customer age, annual income, and transaction amounts

### Statistical Analysis
- Calculates Jensen-Shannon divergence between consecutive snapshots
- JS divergence ranges from 0 (identical distributions) to 1 (completely different)
- Configurable threshold for anomaly detection (default: 0.15)

### Dashboard Features
- **Overview**: Time series of JS divergence with anomaly threshold
- **Detailed Analysis**: Distribution histograms and comparison tables
- **Snapshot Info**: Metadata and statistics for each snapshot

## Key Concepts

**Jensen-Shannon Divergence**: A symmetric measure of similarity between probability distributions. Unlike KL-divergence, it's bounded [0,1] and symmetric.

**Use Cases**:
- ETL pipeline monitoring
- Data drift detection in ML pipelines  
- Regulatory compliance auditing
- Database schema change detection

## Files Created

The demo creates a temporary workspace with:
- `quality_workspace/` - Snapbase workspace directory
- `customer_data.csv` - Synthetic data file
- Snapshot metadata in Hive-style partitioning

## Dependencies

All dependencies are managed via uv with inline metadata:
- `snapbase` - Snapshot database
- `streamlit` - Web dashboard
- `polars` - Fast data manipulation
- `scipy` - Jensen-Shannon divergence calculation
- `plotly` - Interactive charts
- `numpy` - Data generation
- `pandas` - Data conversion