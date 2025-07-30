
"""
Data Quality Dashboard Demo for Snapbase

This demo shows how to use Snapbase with statistical analysis to monitor
data quality over time using Jensen-Shannon divergence.

"""
import streamlit as st
import polars as pl
import numpy as np
import pandas as pd
from scipy.spatial.distance import jensenshannon
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import os
import sys
import shutil
import snapbase


class DataQualityDashboard:
    def __init__(self, workspace_path: str):
        self.workspace_path = workspace_path
        self.workspace = None
        self.snapshots = []
        
    def initialize_workspace(self):
        """Initialize Snapbase workspace"""
        if os.path.exists(self.workspace_path):
            shutil.rmtree(self.workspace_path)
        os.makedirs(self.workspace_path, exist_ok=True)
        
        self.workspace = snapbase.Workspace(self.workspace_path)
        self.workspace.init()
        
    def generate_synthetic_data(self, snapshot_idx: int, n_rows: int = 1000) -> pl.DataFrame:
        """Generate synthetic customer data with controlled distribution changes"""
        np.random.seed(42 + snapshot_idx)
        
        # Base parameters for normal snapshots
        if snapshot_idx != 5:  # Snapshot 5 is the anomaly
            # Normal distribution parameters
            age_mean, age_std = 35, 8
            income_mean, income_std = 50000, 15000
            transaction_mean, transaction_std = 150, 50
        else:
            # Anomalous distribution (significant shift)
            age_mean, age_std = 55, 12  # Older customers
            income_mean, income_std = 80000, 25000  # Higher income
            transaction_mean, transaction_std = 300, 80  # Higher transactions
        
        # Add some temporal drift for realism
        time_drift = snapshot_idx * 0.5
        
        data = {
            'customer_id': range(1, n_rows + 1),
            'age': np.clip(np.random.normal(age_mean + time_drift, age_std, n_rows), 18, 80).astype(int),
            'annual_income': np.clip(np.random.normal(income_mean + time_drift * 1000, income_std, n_rows), 20000, 200000).astype(int),
            'transaction_amount': np.clip(np.random.normal(transaction_mean + time_drift * 5, transaction_std, n_rows), 10, 1000).astype(int),
            'region': np.random.choice(['North', 'South', 'East', 'West'], n_rows, p=[0.3, 0.25, 0.25, 0.2]),
            'timestamp': pd.date_range(
                start=datetime(2024, 1, 1) + timedelta(days=snapshot_idx * 30),
                periods=n_rows,
                freq='1H'
            )
        }
        
        return pl.DataFrame(data)
    
    def create_snapshots(self, n_snapshots: int = 8):
        """Create historical snapshots with one anomaly"""
        self.snapshots = []
        data_file = os.path.join(self.workspace_path, 'customer_data.csv')
        
        for i in range(n_snapshots):
            # Generate data for this snapshot
            df = self.generate_synthetic_data(i)
            
            # Save to CSV
            df.write_csv(data_file)
            
            # Create snapshot in Snapbase
            snapshot_name = f"snapshot_{i+1:02d}"
            self.workspace.create_snapshot('customer_data.csv', name=snapshot_name)
            self.snapshots.append(snapshot_name)
            
        return self.snapshots
    
    def compute_distribution(self, data: np.ndarray, bins: int = 50) -> np.ndarray:
        """Compute probability distribution from data"""
        hist, _ = np.histogram(data, bins=bins, density=True)
        # Normalize to sum to 1 (probability distribution)
        hist = hist / np.sum(hist)
        # Add small epsilon to avoid zeros (causes issues with JS divergence)
        hist = hist + 1e-10
        hist = hist / np.sum(hist)
        return hist
    
    def calculate_js_divergence_for_column(self, column: str) -> list:
        """Calculate Jensen-Shannon divergence for a specific column across all snapshots"""
        divergences = []
        
        for i in range(len(self.snapshots) - 1):
            # Query data for consecutive snapshots
            current_df = self.workspace.query(f"""
                SELECT {column} FROM customer_data_csv 
                WHERE snapshot_name = '{self.snapshots[i]}'
            """)
            
            next_df = self.workspace.query(f"""
                SELECT {column} FROM customer_data_csv 
                WHERE snapshot_name = '{self.snapshots[i+1]}'
            """)
            
            # Convert to numpy arrays
            current_data = current_df[column].to_numpy()
            next_data = next_df[column].to_numpy()
            
            # Compute probability distributions
            dist1 = self.compute_distribution(current_data)
            dist2 = self.compute_distribution(next_data)
            
            # Calculate Jensen-Shannon divergence
            js_div = jensenshannon(dist1, dist2)
            divergences.append(js_div)
            
        return divergences
    
    def get_dashboard_data(self) -> dict:
        """Get all data needed for the dashboard"""
        # Calculate JS divergence for key numeric columns
        columns = ['age', 'annual_income', 'transaction_amount']
        js_data = {}
        
        for column in columns:
            js_data[column] = self.calculate_js_divergence_for_column(column)
        
        # Create comparison pairs
        comparison_pairs = [f"{self.snapshots[i]} → {self.snapshots[i+1]}" 
                          for i in range(len(self.snapshots)-1)]
        
        return {
            'js_divergences': js_data,
            'comparison_pairs': comparison_pairs,
            'snapshots': self.snapshots
        }


def main():
    st.set_page_config(
        page_title="Snapbase Data Quality Dashboard",
        page_icon="📊",
        layout="wide"
    )
    
    st.title("📊 Data Quality Monitoring with Snapbase")
    st.markdown("*Detecting distribution drift using Jensen-Shannon divergence*")
    
    # Initialize session state
    if 'dashboard' not in st.session_state:
        st.session_state.dashboard = None
        st.session_state.data_ready = False
    
    # Sidebar controls
    with st.sidebar:
        st.header("🔧 Controls")
        
        if st.button("🚀 Generate Demo Data", type="primary"):
            with st.spinner("Initializing workspace and creating snapshots..."):
                # Create temporary workspace
                workspace_path = os.path.join(os.getcwd(), 'quality_workspace')
                # Initialize dashboard
                dashboard = DataQualityDashboard(workspace_path)
                dashboard.initialize_workspace()
                # Create snapshots
                snapshots = dashboard.create_snapshots(8)
                
                st.session_state.dashboard = dashboard
                st.session_state.data_ready = True
                
            st.success(f"✅ Created {len(snapshots)} snapshots!")
            st.rerun()
        
        if st.session_state.data_ready:
            st.success("📊 Data ready for analysis")
            
            # Divergence threshold slider
            threshold = st.slider(
                "Anomaly Threshold", 
                min_value=0.0, 
                max_value=1.0, 
                value=0.45, 
                step=0.01,
                help="JS divergence values above this threshold indicate anomalies"
            )
        else:
            st.info("👆 Click 'Generate Demo Data' to start")
            threshold = 0.45
    
    # Main dashboard
    if st.session_state.data_ready:
        dashboard = st.session_state.dashboard
        
        # Get dashboard data
        with st.spinner("Calculating Jensen-Shannon divergences..."):
            data = dashboard.get_dashboard_data()
        
        # Create tabs for different views
        tab1, tab2, tab3 = st.tabs(["📈 Overview", "🔍 Detailed Analysis", "📋 Snapshot Info"])
        
        with tab1:
            st.header("Data Quality Over Time")
            
            # Create combined plot
            fig = go.Figure()
            
            colors = ['#1f77b4', '#ff7f0e', '#2ca02c']
            
            for i, (column, divergences) in enumerate(data['js_divergences'].items()):
                fig.add_trace(go.Scatter(
                    x=list(range(1, len(divergences) + 1)),
                    y=divergences,
                    mode='lines+markers',
                    name=column.replace('_', ' ').title(),
                    line=dict(color=colors[i], width=3),
                    marker=dict(size=8)
                ))
            
            # Add threshold line
            fig.add_hline(
                y=threshold, 
                line_dash="dash", 
                line_color="red",
                annotation_text=f"Anomaly Threshold ({threshold})"
            )
            
            fig.update_layout(
                title="Jensen-Shannon Divergence Between Consecutive Snapshots",
                xaxis_title="Snapshot Transition",
                yaxis_title="JS Divergence",
                height=500,
                hovermode='x unified'
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Alert for anomalies
            anomalies_found = False
            for column, divergences in data['js_divergences'].items():
                for i, div in enumerate(divergences):
                    if div > threshold:
                        anomalies_found = True
                        st.error(f"🚨 **Anomaly Detected!** {column.replace('_', ' ').title()} distribution changed significantly between {data['comparison_pairs'][i]} (JS divergence: {div:.3f})")
            
            if not anomalies_found:
                st.success("✅ No anomalies detected with current threshold")
        
        with tab2:
            st.header("Detailed Distribution Analysis")
            
            # Column selector
            selected_column = st.selectbox(
                "Select column to analyze:",
                ['age', 'annual_income', 'transaction_amount'],
                format_func=lambda x: x.replace('_', ' ').title()
            )
            
            # Show distribution for all snapshots of selected column
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Distribution Comparison")
                
                # Get data for all snapshots
                all_data = []
                for snapshot in data['snapshots']:
                    df = dashboard.workspace.query(f"""
                        SELECT {selected_column} FROM customer_data_csv 
                        WHERE snapshot_name = '{snapshot}'
                    """)
                    df_with_snapshot = df.with_columns(pl.lit(snapshot).alias('snapshot'))
                    all_data.append(df_with_snapshot)
                
                combined_df = pl.concat(all_data)
                combined_pd = combined_df.to_pandas()
                
                # Create histogram
                fig = px.histogram(
                    combined_pd, 
                    x=selected_column, 
                    color='snapshot',
                    title=f"Distribution of {selected_column.replace('_', ' ').title()} Across All Snapshots",
                    marginal="box"
                )
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                st.subheader("JS Divergence Details")
                
                # Create detailed divergence table
                divergences = data['js_divergences'][selected_column]
                pairs = data['comparison_pairs']
                
                df_table = pd.DataFrame({
                    'Transition': pairs,
                    'JS Divergence': [f"{d:.4f}" for d in divergences],
                    'Status': ['🚨 Anomaly' if d > threshold else '✅ Normal' for d in divergences]
                })
                
                st.dataframe(df_table, use_container_width=True)
                
                # Summary statistics
                st.subheader("Summary Statistics")
                st.metric("Average JS Divergence", f"{np.mean(divergences):.4f}")
                st.metric("Maximum JS Divergence", f"{np.max(divergences):.4f}")
                st.metric("Anomalies Detected", len([d for d in divergences if d > threshold]))
        
        with tab3:
            st.header("Snapshot Information")
            
            # List all snapshots
            st.subheader("Available Snapshots")
            
            snapshot_info = []
            for i, snapshot in enumerate(data['snapshots']):
                # Get basic stats for each snapshot
                stats_df = dashboard.workspace.query(f"""
                    SELECT 
                        COUNT(*) as row_count,
                        AVG(age) as avg_age,
                        AVG(annual_income) as avg_income,
                        AVG(transaction_amount) as avg_transaction
                    FROM customer_data_csv 
                    WHERE snapshot_name = '{snapshot}'
                """)
                
                stats = stats_df.to_pandas().iloc[0]
                
                snapshot_info.append({
                    'Snapshot': snapshot,
                    'Rows': int(stats['row_count']),
                    'Avg Age': f"{stats['avg_age']:.1f}",
                    'Avg Income': f"${stats['avg_income']:,.0f}",
                    'Avg Transaction': f"${stats['avg_transaction']:.0f}",
                    'Type': '🚨 Anomalous' if i == 5 else '✅ Normal'
                })
            
            st.dataframe(pd.DataFrame(snapshot_info), use_container_width=True)
            
            # Workspace info
            st.subheader("Workspace Details")
            st.code(f"Workspace Path: {dashboard.workspace_path}")
            st.code(f"Total Snapshots: {len(data['snapshots'])}")
    
    else:
        # Welcome screen
        st.markdown("""
        ## Welcome to the Snapbase Data Quality Demo! 👋
        
        This demo shows how to use **Snapbase** with statistical analysis to monitor data quality over time.
        
        ### What this demo does:
        - 📊 Creates 8 synthetic snapshots of customer data
        - 🧮 Calculates Jensen-Shannon divergence between consecutive snapshots  
        - 🔍 Identifies when data distributions change significantly (anomaly detection)
        - 📈 Visualizes data quality trends over time
        
        ### The data:
        - **7 snapshots** have similar, gradually drifting distributions
        - **1 snapshot** (#6) has a significantly different distribution (the anomaly)
        - Tracks customer age, income, and transaction amounts
        
        ### Getting started:
        Click **"Generate Demo Data"** in the sidebar to create the demo workspace and snapshots.
        """)
        
        # Add some visual elements
        col1, col2, col3 = st.columns(3)
        with col1:
            st.info("🔧 **Step 1**\nGenerate synthetic data")
        with col2:
            st.info("📊 **Step 2**\nCreate Snapbase snapshots")
        with col3:
            st.info("📈 **Step 3**\nAnalyze data quality")


if __name__ == "__main__":
    main()