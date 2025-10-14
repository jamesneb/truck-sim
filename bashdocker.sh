REGION=us-east-1
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text --region $REGION)
REPO_URI="${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/truck-sim:latest"

docker build -t truck-sim:latest .
docker tag  truck-sim:latest "$REPO_URI"
docker push "$REPO_URI"

