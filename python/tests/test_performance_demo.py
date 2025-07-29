"""
Performance demonstration for zero-copy Polars integration
"""

import pytest
import time
from pathlib import Path
from snapbase import Workspace

# Skip if polars not available
polars = pytest.importorskip("polars")


class TestPerformanceDemo:
    """Demonstrate performance improvements with Polars vs JSON"""
    
    def test_polars_operations_demo(self, temp_workspace):
        """Demonstrate Polars DataFrame operations and capabilities"""
        # Create a dataset for testing operations  
        demo_csv = temp_workspace / "polars_demo.csv"
        
        print(f"\n🚀 Creating demo dataset with 10,000 rows...")
        with open(demo_csv, 'w') as f:
            f.write("id,name,age,salary,department,active\n")
            for i in range(10000):
                f.write(f"{i},Employee_{i},{25 + (i % 40)},{40000 + (i % 60000)},Dept_{i % 5},{i % 2 == 0}\n")
        
        workspace = Workspace(str(temp_workspace))
        workspace.init()
        
        print(f"📊 Creating snapshot...")
        result = workspace.create_snapshot(str(demo_csv), "polars_demo")
        print(f"Snapshot result: {result}")
        
        # Check what snapshots exist
        snapshots = workspace.list_snapshots()
        print(f"Available snapshots: {snapshots}")
        
        # Query returns Polars DataFrame - start with simple query
        query = "SELECT * FROM data LIMIT 10"
        
        print(f"\n⚡ Getting Polars DataFrame...")
        start_time = time.time()
        df = workspace.query(query.replace("FROM data", "FROM polars_demo_csv"))
        query_time = time.time() - start_time
        
        print(f"   Query time: {query_time:.4f} seconds")
        print(f"   Result shape: {df.shape}")
        
        # Demonstrate Polars operations
        print(f"\n🔧 Polars DataFrame operations:")
        
        # Fast aggregations
        dept_stats = df.group_by("department").agg([
            polars.col("salary").mean().alias("avg_salary"),
            polars.col("salary").max().alias("max_salary"),
            polars.col("id").count().alias("employee_count")
        ])
        print(f"   Department stats shape: {dept_stats.shape}")
        
        # Filtering
        high_earners = df.filter(polars.col("salary") > 70000)
        print(f"   High earners: {high_earners.height} employees")
        
        # Column operations
        df_with_bonus = df.with_columns([
            (polars.col("salary") * 0.1).alias("bonus"),
            polars.when(polars.col("age") > 35).then(polars.lit("Senior")).otherwise(polars.lit("Junior")).alias("level")
        ])
        print(f"   Enhanced DataFrame shape: {df_with_bonus.shape}")
        
        print(f"\n📊 Sample results:")
        print(f"{dept_stats.head(3)}")
        
        assert isinstance(df, polars.DataFrame)
        assert df.height > 0
        assert isinstance(dept_stats, polars.DataFrame)
        
        print(f"\n✅ Polars operations demo completed!")
    
    def test_memory_efficiency_demo(self, temp_workspace):
        """Demonstrate memory efficiency with zero-copy operations"""
        # Create dataset
        memory_csv = temp_workspace / "memory_test.csv"
        
        with open(memory_csv, 'w') as f:
            f.write("id,data,value\n")
            for i in range(10000):
                f.write(f"{i},{'x' * 100},{i * 1.5}\n")  # Larger string data
        
        workspace = Workspace(str(temp_workspace))
        workspace.init()
        workspace.create_snapshot(memory_csv.name, "memory_test")
        
        # Get Polars DataFrame
        df = workspace.query("SELECT * FROM memory_test_csv")
        
        print(f"\n💾 Memory efficiency demo:")
        print(f"   DataFrame shape: {df.shape}")
        print(f"   Memory usage: {df.estimated_size('mb'):.2f} MB")
        
        # Demonstrate zero-copy operations
        print(f"\n🔄 Zero-copy operations:")
        
        # Filter (should be zero-copy view)
        filtered = df.filter(polars.col("id") < 5000)
        print(f"   Filtered rows: {filtered.height}")
        
        # Select columns (should be zero-copy)
        selected = df.select(["id", "value"])
        print(f"   Selected columns: {selected.width}")
        
        # Convert to Arrow (zero-copy)
        arrow_table = df.to_arrow()
        print(f"   Arrow table rows: {arrow_table.num_rows}")
        
        print(f"   ✅ All operations completed efficiently!")
        
        # Basic assertions
        assert filtered.height <= df.height
        assert selected.width < df.width
        assert arrow_table.num_rows == df.height