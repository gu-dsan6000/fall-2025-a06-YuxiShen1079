# 1. Create necessary directories
mkdir -p data/raw data/output

# 2. Download the dataset archive from the course S3 bucket
# Note: The bucket uses Requester Pays, so you must include --request-payer requester
aws s3 cp s3://dsan6000-datasets/spark-logs/Spark.tar.gz data/raw/ \
  --request-payer requester
echo "finish downloading the dataset from course S3 bucket"

# This will download ~175 MB compressed file

# 3. Extract the archive
cd data/raw/
tar -xzf Spark.tar.gz
cd ../..

# 4. Verify extraction
ls data/raw/application_* | head -10
# Should see directories like: application_1485248649253_0052

# 5. Create your personal S3 bucket (replace YOUR-NET-ID with your actual net ID)
export YOUR_NET_ID="ys1079"  # e.g., "abc123"
aws s3 mb s3://${YOUR_NET_ID}-assignment-spark-cluster-logs

# 6. Upload the data to S3
aws s3 cp data/raw/ s3://${YOUR_NET_ID}-assignment-spark-cluster-logs/data/ \
  --recursive \
  --exclude "*.gz" \
  --exclude "*.tar.gz"

# 7. Verify upload
aws s3 ls s3://${YOUR_NET_ID}-assignment-spark-cluster-logs/data/ | head -10
# You should see application directories listed
# Note: "Broken pipe" error at the end is normal (caused by head command)

# 8. Save your bucket name for later use
echo "export SPARK_LOGS_BUCKET=s3://${YOUR_NET_ID}-assignment-spark-cluster-logs" >> ~/.bashrc
source ~/.bashrc