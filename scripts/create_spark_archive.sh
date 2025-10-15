#!/bin/bash
# Create a Spark library archive to reduce staging overhead
# This archive will be uploaded once to HDFS and reused by all Spark jobs
# Usage: bash scripts/create_spark_archive.sh

#!/bin/bash
# Create a Spark library archive to reduce staging overhead
# This archive will be uploaded once to HDFS and reused by all Spark jobs
# Usage: bash scripts/create_spark_archive.sh

set -euo pipefail

SPARK_HOME="${SPARK_HOME:-/home/dlhnhom2/spark}"
ARCHIVE_NAME="spark-libs.zip"
HDFS_PATH="hdfs://khoa-master:9000/user/${USER}/${ARCHIVE_NAME}"
LOCAL_TEMP="/tmp/${ARCHIVE_NAME}"

echo "=========================================="
echo "Creating Spark Library Archive"
echo "=========================================="
echo "SPARK_HOME: $SPARK_HOME"
echo "Archive: $ARCHIVE_NAME"
echo "HDFS destination: $HDFS_PATH"
echo "=========================================="

# Check if SPARK_HOME exists
if [ ! -d "$SPARK_HOME" ]; then
  echo "ERROR: SPARK_HOME directory not found: $SPARK_HOME"
  echo "Set SPARK_HOME environment variable or edit this script"
  exit 1
fi

# Create archive from Spark jars
echo ""
echo "Step 1: Creating local archive..."
cd "$SPARK_HOME"

if [ -d "jars" ]; then
  echo "Archiving jars directory into ZIP using Python (flattening to archive root)..."
  # Use Python to create a zip with jar files at the archive root (no 'jars/' prefix)
  python3 - <<PY
import os, zipfile
spark_home = os.environ.get('SPARK_HOME', '/home/dlhnhom2/spark')
src = os.path.join(spark_home, 'jars')
dst = os.path.abspath(os.path.expanduser('${LOCAL_TEMP}'))
with zipfile.ZipFile(dst, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
    for root, dirs, files in os.walk(src):
        for f in files:
            if f.endswith('.jar'):
                full = os.path.join(root, f)
                # Put jar files at the root of the archive (no 'jars/' prefix)
                zf.write(full, arcname=f)
print('Wrote', dst)
PY

  if [ $? -ne 0 ]; then
    echo "ERROR: failed to create zip archive with python3"; exit 1; fi
  echo "✓ Created $LOCAL_TEMP"
else
  echo "ERROR: $SPARK_HOME/jars directory not found"
  exit 1
fi

# Get archive size
ARCHIVE_SIZE=$(du -h "$LOCAL_TEMP" | awk '{print $1}')
echo "Archive size: $ARCHIVE_SIZE"

# Upload to HDFS
echo ""
echo "Step 2: Uploading to HDFS..."
if hdfs dfs -test -e "$HDFS_PATH" 2>/dev/null; then
  echo "Archive already exists in HDFS. Removing old version..."
  hdfs dfs -rm -skipTrash "$HDFS_PATH" || true
fi

hdfs dfs -put -f "$LOCAL_TEMP" "$HDFS_PATH" || { echo "ERROR uploading to HDFS"; exit 1; }
echo "✓ Uploaded to $HDFS_PATH"

# Verify
echo ""
echo "Step 3: Verifying..."
HDFS_SIZE=$(hdfs dfs -du -h "$HDFS_PATH" | awk '{print $1}')
echo "HDFS archive size: $HDFS_SIZE (x replication)"

# Cleanup local temp
rm -f "$LOCAL_TEMP"
echo "✓ Cleaned up local temp file"

echo ""
echo "=========================================="
echo "SUCCESS!"
echo "=========================================="
echo "Archive created and uploaded to HDFS." 
echo ""
echo "Your spark_submit.sh is already configured to use:"
echo "  spark.yarn.archive=$HDFS_PATH"
echo ""
echo "This will reduce staging overhead from ~440MB to <10MB per job."
echo "=========================================="
