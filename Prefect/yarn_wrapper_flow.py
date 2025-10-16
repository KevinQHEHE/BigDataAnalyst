"""Prefect Wrapper Flow for YARN Execution.

This flow calls spark-submit to run the actual pipeline on YARN cluster.
This is the recommended way to integrate Prefect scheduling with YARN execution.
"""
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Dict

from prefect import flow, task


@task(name="run_pipeline_on_yarn", retries=2, retry_delay_seconds=300)
def run_pipeline_on_yarn_task(
    flow_script: str = "Prefect/full_pipeline_flow.py",
    mode: str = "hourly"
) -> Dict:
    """Execute pipeline on YARN via spark-submit wrapper.
    
    Args:
        flow_script: Path to the flow script to run
        mode: 'hourly' or custom args
        
    Returns:
        Dictionary with execution results
    """
    import os
    
    # Get project root
    root_dir = Path(__file__).resolve().parent.parent
    os.chdir(root_dir)
    
    print(f"="*80)
    print(f"Running {flow_script} on YARN")
    print(f"Mode: {mode}")
    print(f"Working directory: {root_dir}")
    print(f"Timestamp: {datetime.now().isoformat()}")
    print(f"="*80)
    
    # Build command
    if mode == "hourly":
        cmd = [
            "bash", 
            "scripts/spark_submit.sh",
            flow_script,
            "--",
            "--hourly"
        ]
    else:
        cmd = [
            "bash",
            "scripts/spark_submit.sh", 
            flow_script
        ]
    
    print(f"\nExecuting: {' '.join(cmd)}\n")
    
    # Run command with streaming output (reduces memory usage)
    start_time = datetime.now()
    
    try:
        # Use Popen to stream output in real-time instead of buffering
        process = subprocess.Popen(
            cmd,
            cwd=root_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,  # Merge stderr into stdout
            text=True,
            bufsize=1,  # Line buffered
            universal_newlines=True
        )
        
        # Stream output line by line (memory efficient)
        output_lines = []
        max_lines_to_keep = 100  # Keep only last 100 lines for error reporting
        
        for line in process.stdout:
            print(line, end='')  # Print immediately without buffering
            output_lines.append(line)
            # Keep only recent lines to avoid memory growth
            if len(output_lines) > max_lines_to_keep:
                output_lines.pop(0)
        
        # Wait for process to complete (with timeout)
        return_code = process.wait(timeout=3600)  # 1 hour timeout
        duration = (datetime.now() - start_time).total_seconds()
        
        # Check result
        if return_code == 0:
            print(f"\n{'='*80}")
            print(f"✓ Pipeline completed successfully in {duration:.1f}s")
            print(f"{'='*80}")
            
            return {
                "success": True,
                "duration_seconds": duration,
                "return_code": return_code,
                "timestamp": datetime.now().isoformat()
            }
        else:
            error_msg = f"Pipeline failed with return code {return_code}"
            print(f"\n{'='*80}")
            print(f"✗ {error_msg}")
            print(f"Last {len(output_lines)} lines of output:")
            print(''.join(output_lines))
            print(f"{'='*80}")
            
            raise RuntimeError(error_msg)
            
    except subprocess.TimeoutExpired:
        # Kill process if still running
        if process.poll() is None:
            process.kill()
            process.wait()
        
        duration = (datetime.now() - start_time).total_seconds()
        error_msg = f"Pipeline timed out after {duration:.1f}s"
        print(f"\n{'='*80}")
        print(f"✗ {error_msg}")
        print(f"{'='*80}")
        raise RuntimeError(error_msg)
    
    except Exception as e:
        # Kill process if still running
        if 'process' in locals() and process.poll() is None:
            process.kill()
            process.wait()
        
        duration = (datetime.now() - start_time).total_seconds()
        error_msg = f"Pipeline failed: {e}"
        print(f"\n{'='*80}")
        print(f"✗ {error_msg}")
        print(f"{'='*80}")
        raise


@flow(
    name="Hourly Pipeline on YARN",
    description="Runs hourly AQI pipeline on YARN via spark-submit",
    log_prints=True,
    retries=1,
    retry_delay_seconds=600
)
def hourly_pipeline_yarn_flow() -> Dict:
    """Execute hourly pipeline on YARN cluster.
    
    This flow wraps the spark-submit command to enable:
    - Prefect scheduling and monitoring
    - YARN execution with proper resource allocation
    - Automatic retries on failure
    - Comprehensive logging
    
    Returns:
        Dictionary with pipeline execution results
    """
    print("="*80)
    print("PREFECT HOURLY PIPELINE (YARN MODE)")
    print(f"Started: {datetime.now().isoformat()}")
    print("="*80)
    
    result = run_pipeline_on_yarn_task(
        flow_script="Prefect/full_pipeline_flow.py",
        mode="hourly"
    )
    
    return result


@flow(
    name="Full Pipeline on YARN",
    description="Runs full AQI pipeline on YARN via spark-submit",
    log_prints=True,
    retries=1,
    retry_delay_seconds=600
)
def full_pipeline_yarn_flow() -> Dict:
    """Execute full pipeline on YARN cluster.
    
    Returns:
        Dictionary with pipeline execution results
    """
    print("="*80)
    print("PREFECT FULL PIPELINE (YARN MODE)")
    print(f"Started: {datetime.now().isoformat()}")
    print("="*80)
    
    result = run_pipeline_on_yarn_task(
        flow_script="Prefect/full_pipeline_flow.py",
        mode="full"
    )
    
    return result


if __name__ == "__main__":
    # Test run
    hourly_pipeline_yarn_flow()
