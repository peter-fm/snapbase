"""
Test performance with large query results where Polars excels
"""

import pytest
import time
from snapbase import Workspace

# Skip if polars not available
polars = pytest.importorskip("polars")


def test_large_result_set_performance(temp_workspace):
    """Test performance when query returns large result sets"""
    # Create dataset
    large_csv = temp_workspace / "large_results_test.csv"
    
    print(f"\n🚀 Creating dataset with 25,000 rows...")
    with open(large_csv, 'w') as f:
        f.write("id,name,category,value,description\n")
        for i in range(25000):
            f.write(f"{i},Item_{i},Cat_{i%50},{i*1.5},Description for item {i}\n")
    
    workspace = Workspace(str(temp_workspace))
    workspace.init()
    workspace.create_snapshot(str(large_csv), "large_results_test")
    
    # Query that returns most of the data (this is where Polars should excel)
    query = "SELECT * FROM data WHERE id % 3 = 0 ORDER BY id"  # Returns ~8,333 rows
    
    print(f"📊 Running query that returns ~8,333 rows...")
    
    # Time Polars method
    print(f"⚡ Testing Polars DataFrame method...")
    start_time = time.time()
    polars_result = workspace.query(query.replace("FROM data", "FROM large_results_test_csv"))
    polars_time = time.time() - start_time
    
    print(f"\n📈 Large Result Set Performance:")
    print(f"   Result rows:     {polars_result.height}")
    print(f"   Result columns:  {polars_result.width}")
    print(f"   Query time:      {polars_time:.4f} seconds")
    print(f"   Rows per second: {polars_result.height / polars_time:.0f}")
    
    print(f"\n🎯 Benefits of Polars approach:")
    print(f"   - Zero-copy Arrow data transfer")
    print(f"   - Columnar memory layout")
    print(f"   - Efficient operations on result")
    print(f"   - Direct integration with data science tools")
    
    # Demonstrate Polars capabilities
    print(f"\n🔧 Polars operations on result:")
    
    # Fast aggregations
    avg_value = polars_result.select(polars.col("value").mean()).item()
    print(f"   Average value: {avg_value:.2f}")
    
    # Fast filtering
    high_values = polars_result.filter(polars.col("value") > avg_value).height
    print(f"   Above average: {high_values} rows")
    
    # Memory efficiency
    memory_mb = polars_result.estimated_size('mb')
    print(f"   Memory usage:  {memory_mb:.2f} MB")
    
    # Verify we have substantial results
    assert polars_result.height > 5000  # Should have substantial data
    assert polars_result.width > 3      # Should have multiple columns
    
    print(f"\n✅ Large result set test completed!")


if __name__ == "__main__":
    # Can be run directly for manual testing
    import tempfile
    from pathlib import Path
    
    with tempfile.TemporaryDirectory() as tmp_dir:
        temp_workspace = Path(tmp_dir)
        test_large_result_set_performance(temp_workspace)