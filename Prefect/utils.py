"""Shared utilities for Prefect flows.

Contains common functions used across multiple flows.
"""
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, Tuple, List


def run_subprocess_job(
    script_path: str,
    args: List[str],
    job_name: str = "job",
    timeout: int = 3600,
    root_dir: Path = None
) -> Tuple[bool, Dict]:
    """Run a Spark job as subprocess with fresh JVM.
    
    Executes a Python script via spark-submit wrapper for YARN deployment.
    Each job runs in a separate JVM process ensuring:
    - Fresh heap memory
    - Efficient garbage collection
    - No resource contention between stages
    
    Args:
        script_path: Relative path to script (e.g., "jobs/bronze/run_bronze_pipeline.py")
        args: Command-line arguments to pass to the script
        job_name: Name for logging purposes
        timeout: Timeout in seconds (default: 3600s = 1 hour)
        root_dir: Project root directory (defaults to parent of Prefect folder)
        
    Returns:
        Tuple of (success: bool, result: dict)
        - success: True if return_code == 0, False otherwise
        - result: Dictionary with 'success', 'elapsed_seconds', and error info if failed
        
    Example:
        >>> success, result = run_subprocess_job(
        ...     "jobs/bronze/run_bronze_pipeline.py",
        ...     ["--mode", "upsert"],
        ...     "Bronze",
        ...     timeout=3600
        ... )
    """
    if root_dir is None:
        root_dir = Path(__file__).resolve().parent.parent
    
    start_time = time.time()
    script_full = root_dir / script_path
    
    if not script_full.exists():
        return False, {"error": f"Script not found: {script_full}"}
    
    cmd = [
        "bash",
        str(root_dir / "scripts/spark_submit.sh"),
        str(script_path),
        "--"
    ] + args
    
    print(f"\n[{job_name}] Starting subprocess (fresh JVM)...")
    print(f"[{job_name}] Command: {' '.join(cmd[-4:])}")
    print(f"[{job_name}] Output:")
    print("-" * 80)
    sys.stdout.flush()
    
    try:
        # Use Popen for real-time output streaming (prevents Prefect lag)
        # This allows Prefect to see logs as they happen without buffering
        process = subprocess.Popen(
            cmd,
            cwd=str(root_dir),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1  # Line buffered for real-time output
        )
        
        # Stream output line by line
        for line in process.stdout:
            print(line, end='', flush=True)  # Explicit flush for Prefect logging
        
        # Wait for process to complete
        return_code = process.wait(timeout=timeout)
        elapsed = time.time() - start_time
        print("-" * 80)
        sys.stdout.flush()
        
        if return_code == 0:
            print(f"[{job_name}] SUCCESS ({elapsed:.1f}s)")
            return True, {
                "success": True,
                "elapsed_seconds": elapsed
            }
        else:
            print(f"[{job_name}] FAILED (exit {return_code})")
            return False, {
                "success": False,
                "exit_code": return_code,
                "elapsed_seconds": elapsed
            }
    
    except subprocess.TimeoutExpired:
        print("-" * 80)
        print(f"[{job_name}] TIMEOUT ({timeout}s)")
        process.kill()
        return False, {
            "success": False,
            "error": f"Timeout after {timeout}s",
            "elapsed_seconds": timeout
        }
    except Exception as e:
        print("-" * 80)
        print(f"[{job_name}] EXCEPTION: {e}")
        return False, {
            "success": False,
            "error": str(e),
            "elapsed_seconds": time.time() - start_time
        }
