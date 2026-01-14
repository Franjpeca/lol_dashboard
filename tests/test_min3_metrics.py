"""
Simple test to verify metrics execution for min=3 without requiring MongoDB connection.
This test focuses on file-based verification and command execution.
"""
import os
import sys
import subprocess
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = BASE_DIR / "src"
RESULTS_DIR = BASE_DIR / "data" / "results" / "pool_ac89fa8d" / "q440"

def check_metrics_files(min_friends):
    """Check which metric files exist for a given min_friends value."""
    metrics_dir = RESULTS_DIR / f"min{min_friends}"
    
    expected_metrics = [
        "metrics_01_players_games_winrate.json",
        "metrics_02_champions_games_winrate.json",
        "metrics_03_games_frecuency.json",
        "metrics_04_win_lose_streak.json",
        "metrics_05_players_stats.json",
        "metrics_06_ego_index.json",
        "metrics_07_troll_index.json",
        "metrics_08_first_metrics.json",
        "metrics_09_number_skills.json",
        "metrics_10_stats_by_rol.json",
        "metrics_11_stats_record.json",
        "metrics_12_botlane_synergy.json",
        "metrics_13_player_champions_stats.json",
    ]
    
    print(f"\n{'='*70}")
    print(f"Checking metrics files for min{min_friends} in:")
    print(f"  {metrics_dir}")
    print(f"{'='*70}\n")
    
    if not metrics_dir.exists():
        print(f"❌ Directory does not exist!")
        return []
    
    missing = []
    existing = []
    
    for metric in expected_metrics:
        metric_path = metrics_dir / metric
        if metric_path.exists():
            size = metric_path.stat().st_size
            existing.append((metric, size))
            print(f"  ✓ {metric:<45} ({size:>10,} bytes)")
        else:
            missing.append(metric)
            print(f"  ❌ {metric:<45} MISSING")
    
    print(f"\n{'='*70}")
    print(f"Summary: {len(existing)}/{len(expected_metrics)} metrics exist for min{min_friends}")
    print(f"{'='*70}")
    
    if missing:
        print(f"\nMissing metrics:")
        for m in missing:
            print(f"  - {m}")
    
    return missing

def compare_min_configs():
    """Compare metrics between different min_friends configurations."""
    print(f"\n{'='*70}")
    print(f"COMPARING METRICS ACROSS MIN_FRIENDS CONFIGURATIONS")
    print(f"{'='*70}\n")
    
    configs = [3, 4, 5]
    results = {}
    
    for min_val in configs:
        missing = check_metrics_files(min_val)
        results[min_val] = missing
    
    print(f"\n{'='*70}")
    print(f"COMPARISON SUMMARY")
    print(f"{'='*70}\n")
    
    for min_val, missing in results.items():
        status = "✓ COMPLETE" if not missing else f"❌ MISSING {len(missing)} metrics"
        print(f"  min{min_val}: {status}")
    
    return results

def test_metricsMain_execution():
    """Test running metricsMain.py for min=3."""
    print(f"\n{'='*70}")
    print(f"TESTING metricsMain.py EXECUTION FOR MIN=3")
    print(f"{'='*70}\n")
    
    script_path = SRC_DIR / "metrics" / "metricsMain.py"
    cmd = [
        sys.executable,
        str(script_path),
        "--queue", "440",
        "--min", "3",
        "--pool", "ac89fa8d"
    ]
    
    print(f"Command: {' '.join(cmd)}\n")
    print(f"{'='*70}")
    print("EXECUTION OUTPUT:")
    print(f"{'='*70}\n")
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=120,
            encoding='utf-8',
            errors='replace'
        )
        
        print(result.stdout)
        
        if result.stderr:
            print(f"\n{'='*70}")
            print("STDERR:")
            print(f"{'='*70}\n")
            print(result.stderr)
        
        print(f"\n{'='*70}")
        if result.returncode == 0:
            print("✓ metricsMain.py executed successfully")
            return True
        else:
            print(f"❌ metricsMain.py failed with exit code {result.returncode}")
            return False
    except subprocess.TimeoutExpired:
        print(f"\n❌ Command timed out after 120 seconds")
        return False
    except Exception as e:
        print(f"\n❌ Error running metricsMain.py: {e}")
        return False

if __name__ == "__main__":
    print("\n" + "="*70)
    print("MIN3 METRICS DIAGNOSTIC TEST")
    print("="*70)
    
    # First, compare existing files
    comparison_results = compare_min_configs()
    
    # Check if min3 is missing any metrics
    min3_missing = comparison_results.get(3, [])
    
    if min3_missing:
        print(f"\n{'='*70}")
        print(f"⚠️  MIN3 IS MISSING {len(min3_missing)} METRICS")
        print(f"{'='*70}")
        print("\nAttempting to regenerate metrics for min3...")
        
        # Try to run metricsMain for min3
        success = test_metricsMain_execution()
        
        if success:
            print("\n" + "="*70)
            print("Verifying metrics after execution...")
            print("="*70)
            
            # Check again
            missing_after = check_metrics_files(3)
            
            if not missing_after:
                print("\n" + "="*70)
                print("✓ ALL METRICS SUCCESSFULLY GENERATED FOR MIN3")
                print("="*70 + "\n")
                sys.exit(0)
            else:
                print("\n" + "="*70)
                print(f"❌ STILL MISSING {len(missing_after)} METRICS AFTER EXECUTION")
                print("="*70 + "\n")
                sys.exit(1)
        else:
            print("\n" + "="*70)
            print("❌ FAILED TO EXECUTE metricsMain.py")
            print("="*70 + "\n")
            sys.exit(1)
    else:
        print("\n" + "="*70)
        print("✓ ALL METRICS EXIST FOR MIN3")
        print("="*70 + "\n")
        sys.exit(0)
