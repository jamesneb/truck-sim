#!/usr/bin/env bash
set -euo pipefail

# --- Configuration -----------------------------------------------------
REGION="us-east-1"
CLUSTER="truck-sim-cluster"
SUBNET_ID="subnet-0faf5b311289ea0af"
SEC_GROUP="sg-03b208d4bb055a8c9"
FAMILY="truck-sim"
IMAGE_TAG="latest"
LOG_GROUP="/ecs/truck-sim"
# ----------------------------------------------------------------------

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text --region "$REGION")
IMAGE_URI="${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${FAMILY}:${IMAGE_TAG}"

echo "🔧 Building and pushing image to ECR: $IMAGE_URI"

# Build image using default builder
echo "🔨 Building Docker image..."
docker build -t "${FAMILY}:${IMAGE_TAG}" .

docker tag "${FAMILY}:${IMAGE_TAG}" "$IMAGE_URI"
aws ecr get-login-password --region us-east-1 | docker login \
  --username AWS \
  --password-stdin 596369789936.dkr.ecr.us-east-1.amazonaws.com

docker push "$IMAGE_URI"

echo "📦 Fetching latest task definition for '$FAMILY'..."
LATEST_TASKDEF=$(aws ecs describe-task-definition \
  --task-definition "$FAMILY" \
  --region "$REGION")

echo "✏️  Creating new task def revision with updated image..."
NEW_TASKDEF=$(echo "$LATEST_TASKDEF" | jq --arg IMG "$IMAGE_URI" '
  .taskDefinition |
  {
    family,
    networkMode,
    containerDefinitions,
    requiresCompatibilities,
    cpu,
    memory,
    executionRoleArn,
    taskRoleArn
  } |
  .containerDefinitions[0].image = $IMG |
  {
    family,
    networkMode,
    containerDefinitions,
    requiresCompatibilities,
    cpu,
    memory,
    executionRoleArn,
    taskRoleArn
  }')

NEW_REVISION=$(aws ecs register-task-definition \
  --cli-input-json "$(echo "$NEW_TASKDEF")" \
  --region "$REGION" \
  --query "taskDefinition.taskDefinitionArn" \
  --output text)

echo "🚀 Launching one-off ECS task using new revision..."
TASK_ARN=$(aws ecs run-task \
  --cluster "$CLUSTER" \
  --task-definition "$NEW_REVISION" \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[$SUBNET_ID],securityGroups=[$SEC_GROUP],assignPublicIp=ENABLED}" \
  --region "$REGION" \
  --query "tasks[0].taskArn" --output text)

echo "✅ Task launched: $TASK_ARN"
echo "📜 Tail logs with:"
echo "    aws logs tail $LOG_GROUP --follow --region $REGION"
