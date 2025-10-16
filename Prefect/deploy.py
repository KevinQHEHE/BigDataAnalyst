"""Prefect Deployment Helper for DLH-AQI Pipeline.

This script provides guidance for deploying Prefect flows using Prefect 3.x.

Prefect 3.x uses flow.deploy() or flow.serve() instead of Deployment class.
For production deployment, use prefect.yaml configuration file.

Usage:
  # For Prefect 3.x, use flow.deploy() or prefect deploy CLI:
  
  1. Using flow.deploy() in code:
     Add to your flow file:
       if __name__ == "__main__":
           flow.deploy(
               name="aqi-pipeline-hourly",
               cron="0 * * * *",
               timezone="Asia/Ho_Chi_Minh"
           )
  
  2. Using prefect deploy CLI:
     prefect deploy Prefect/full_pipeline_flow.py:hourly_pipeline_flow \\
       --name aqi-pipeline-hourly \\
       --cron "0 * * * *"
  
  3. Manual runs via spark_submit.sh:
     bash scripts/spark_submit.sh Prefect/full_pipeline_flow.py -- --hourly

For detailed deployment instructions, see:
- docs/PREFECT_DEPLOYMENT.md
- Prefect 3.x documentation: https://docs.prefect.io/3.0/
"""
import sys

print("=" * 80)
print("PREFECT DEPLOYMENT HELPER - DLH-AQI PIPELINE")
print("=" * 80)
print()
print("⚠️  Note: Prefect 3.x has changed deployment API")
print()
print("For Prefect 3.x (version 3.0+), deployments are created differently:")
print()
print("Option 1: Use 'prefect deploy' CLI (requires work pool)")
print("-" * 80)
print("  # Step 1: Create work pool")
print("  prefect work-pool create default-pool --type process")
print()
print("  # Step 2: Deploy flow")
print("  prefect deploy Prefect/full_pipeline_flow.py:hourly_pipeline_flow \\")
print("    --name aqi-pipeline-hourly \\")
print("    --pool default-pool \\")
print("    --cron '0 * * * *' \\")
print("    --timezone 'Asia/Ho_Chi_Minh'")
print()
print("  # Step 3: Start worker")
print("  prefect worker start --pool default-pool")
print()
print("Option 2: Direct execution via spark_submit.sh (Recommended)")
print("-" * 80)
print("  # Hourly pipeline")
print("  bash scripts/spark_submit.sh Prefect/full_pipeline_flow.py -- --hourly")
print()
print("  # Backfill")
print("  bash scripts/spark_submit.sh Prefect/backfill_flow.py -- \\")
print("    --start-date 2024-01-01 \\")
print("    --end-date 2024-12-31 \\")
print("    --chunk-mode monthly")
print()
print("Option 3: Set up cron job for hourly runs")
print("-" * 80)
print("  # Add to crontab:")
print("  0 * * * * cd /home/dlhnhom2/dlh-aqi && bash scripts/spark_submit.sh Prefect/full_pipeline_flow.py -- --hourly")
print()
print("=" * 80)
print("For detailed instructions, see: docs/PREFECT_DEPLOYMENT.md")
print("=" * 80)

sys.exit(0)


def create_hourly_deployment(update: bool = False) -> Deployment:
    """Create deployment for hourly pipeline.
    
    This deployment runs every hour to:
    - Ingest latest data (Bronze upsert)
    - Transform to Silver (incremental)
    - Update Gold tables (all dims + facts)
    
    Args:
        update: If True, update existing deployment
        
    Returns:
        Deployment object
    """
    from full_pipeline_flow import hourly_pipeline_flow
    
    print("Creating hourly pipeline deployment...")
    
    deployment = Deployment.build_from_flow(
        flow=hourly_pipeline_flow,
        name="aqi-pipeline-hourly",
        version="1.0",
        description="Hourly AQI data pipeline: Bronze upsert → Silver incremental → Gold update",
        tags=["production", "hourly", "scheduled"],
        parameters={
            "warehouse": "hdfs://khoa-master:9000/warehouse/iceberg",
            "require_yarn": True
        },
        schedule=CronSchedule(
            cron="0 * * * *",  # Every hour at minute 0
            timezone="Asia/Ho_Chi_Minh"
        ),
        work_pool_name="default-agent-pool",
        work_queue_name="default",
        is_schedule_active=True
    )
    
    if update:
        print("Updating existing deployment...")
        deployment.apply(upload=True)
    else:
        print("Creating new deployment...")
        deployment.apply(upload=True)
    
    print(f"✓ Deployment created: {deployment.name}")
    print(f"  Schedule: Every hour (0 * * * *)")
    print(f"  Timezone: Asia/Ho_Chi_Minh")
    print(f"  Work Pool: {deployment.work_pool_name}")
    
    return deployment


def create_full_pipeline_deployment(update: bool = False) -> Deployment:
    """Create deployment for full pipeline (manual runs).
    
    This deployment is for manual/ad-hoc runs with custom parameters.
    No schedule attached.
    
    Args:
        update: If True, update existing deployment
        
    Returns:
        Deployment object
    """
    from full_pipeline_flow import full_pipeline_flow
    
    print("Creating full pipeline deployment...")
    
    deployment = Deployment.build_from_flow(
        flow=full_pipeline_flow,
        name="aqi-pipeline-full",
        version="1.0",
        description="Full AQI pipeline with custom parameters (manual runs)",
        tags=["production", "manual", "full-pipeline"],
        parameters={
            "bronze_mode": "upsert",
            "silver_mode": "incremental",
            "gold_mode": "all",
            "warehouse": "hdfs://khoa-master:9000/warehouse/iceberg",
            "require_yarn": True
        },
        work_pool_name="default-agent-pool",
        work_queue_name="default"
    )
    
    if update:
        deployment.apply(upload=True)
    else:
        deployment.apply(upload=True)
    
    print(f"✓ Deployment created: {deployment.name}")
    print(f"  Schedule: None (manual runs)")
    print(f"  Work Pool: {deployment.work_pool_name}")
    
    return deployment


def create_backfill_deployment(update: bool = False) -> Deployment:
    """Create deployment for backfill flow (manual runs).
    
    This deployment is for historical data backfill with date range.
    No schedule attached.
    
    Args:
        update: If True, update existing deployment
        
    Returns:
        Deployment object
    """
    from backfill_flow import backfill_flow
    
    print("Creating backfill deployment...")
    
    deployment = Deployment.build_from_flow(
        flow=backfill_flow,
        name="aqi-pipeline-backfill",
        version="1.0",
        description="Historical data backfill with chunking (manual runs)",
        tags=["production", "manual", "backfill"],
        parameters={
            "chunk_mode": "monthly",
            "include_bronze": True,
            "include_silver": True,
            "include_gold": True,
            "warehouse": "hdfs://khoa-master:9000/warehouse/iceberg",
            "require_yarn": True
        },
        work_pool_name="default-agent-pool",
        work_queue_name="default"
    )
    
    if update:
        deployment.apply(upload=True)
    else:
        deployment.apply(upload=True)
    
    print(f"✓ Deployment created: {deployment.name}")
    print(f"  Schedule: None (manual runs with --start-date --end-date)")
    print(f"  Work Pool: {deployment.work_pool_name}")
    
    return deployment


def deploy_all(update: bool = False):
    """Deploy all flows.
    
    Args:
        update: If True, update existing deployments
    """
    print("="*80)
    print("DEPLOYING ALL PREFECT FLOWS")
    print("="*80)
    
    deployments = []
    
    print("\n1. Hourly Pipeline")
    print("-"*80)
    deployments.append(create_hourly_deployment(update))
    
    print("\n2. Full Pipeline")
    print("-"*80)
    deployments.append(create_full_pipeline_deployment(update))
    
    print("\n3. Backfill")
    print("-"*80)
    deployments.append(create_backfill_deployment(update))
    
    print("\n" + "="*80)
    print("DEPLOYMENT SUMMARY")
    print("="*80)
    for dep in deployments:
        print(f"✓ {dep.name}")
    
    print("\nNext steps:")
    print("1. Start Prefect agent:")
    print("   prefect agent start -p default-agent-pool")
    print("\n2. View deployments in UI:")
    print("   prefect server start")
    print("   Open: http://localhost:4200")
    print("\n3. Manually run a flow:")
    print("   prefect deployment run 'hourly-pipeline-flow/aqi-pipeline-hourly'")
    print("   prefect deployment run 'backfill-flow/aqi-pipeline-backfill' \\")
    print("     --param start_date=2024-01-01 --param end_date=2024-12-31")


def main():
    parser = argparse.ArgumentParser(description="Deploy Prefect flows with scheduling")
    parser.add_argument(
        "--flow",
        choices=["hourly", "full", "backfill"],
        help="Specific flow to deploy"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Deploy all flows"
    )
    parser.add_argument(
        "--update",
        action="store_true",
        help="Update existing deployment (upsert mode)"
    )
    
    args = parser.parse_args()
    
    if not args.flow and not args.all:
        parser.error("Specify --flow <name> or --all")
    
    try:
        if args.all:
            deploy_all(args.update)
        elif args.flow == "hourly":
            create_hourly_deployment(args.update)
        elif args.flow == "full":
            create_full_pipeline_deployment(args.update)
        elif args.flow == "backfill":
            create_backfill_deployment(args.update)
        
        print("\n✓ Deployment complete!")
        
    except Exception as e:
        print(f"\n✗ Deployment failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
