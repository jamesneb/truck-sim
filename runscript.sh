#!/usr/bin/env bash
set -euo pipefail

###############################################################################
# 0. EDIT JUST THESE THREE VALUES                                             #
###############################################################################
REGION="us-east-1"                               # your AWS Region
PUBLIC_SUBNET_ID="subnet-0faf5b311289ea0af"      # Test-VPC public subnet
SECURITY_GROUP_ID="sg-03b208d4bb055a8c9"         # allows outbound HTTPS
###############################################################################

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text --region "$REGION")
REPO_URI="${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/truck-sim:latest"

# ──────────────────────────────────────────────────────────────────────────────
# 1. REGISTER (or update) TASK DEFINITION
# ──────────────────────────────────────────────────────────────────────────────
cat > /tmp/taskdef.json <<EOF
{
  "family": "truck-sim",
  "networkMode": "awsvpc",
  "cpu": "512",
  "memory": "1024",
  "requiresCompatibilities": ["FARGATE"],
  "executionRoleArn": "arn:aws:iam::${ACCOUNT_ID}:role/ecsTaskExecutionRole",
  "taskRoleArn":       "arn:aws:iam::${ACCOUNT_ID}:role/ecsTaskExecutionRole",
  "containerDefinitions": [
    {
      "name": "truck-sim",
      "image": "${REPO_URI}",
      "essential": true,
      "logConfiguration": {
        "logDriver": "awslogs",
        "options": {
          "awslogs-region":  "${REGION}",
          "awslogs-group":   "/ecs/truck-sim",
          "awslogs-stream-prefix": "ecs"
        }
      }
    }
  ]
}
EOF

echo "📦  Registering task definition…"
TASK_DEF_ARN=$(aws ecs register-task-definition \
                  --cli-input-json file:///tmp/taskdef.json \
                  --query 'taskDefinition.taskDefinitionArn' \
                  --output text \
                  --region "$REGION")
echo "    → $TASK_DEF_ARN"

# ──────────────────────────────────────────────────────────────────────────────
# 2. ENSURE CLUSTER EXISTS
# ──────────────────────────────────────────────────────────────────────────────
aws ecs create-cluster --cluster-name truck-sim-cluster --region "$REGION" \
  >/dev/null 2>&1 || echo "ℹ️  Cluster already exists"

# ──────────────────────────────────────────────────────────────────────────────
# 3. CREATE / UPDATE EVENTBRIDGE SCHEDULER
# ──────────────────────────────────────────────────────────────────────────────
SCHED_NAME="truck-sim-nightly"
CRON_EXPR="cron(0 5 * * ? *)"   # 05:00 UTC = 01:00 AM EDT

read -r -d '' TARGET_JSON <<JSON
{
  "Arn": "arn:aws:ecs:${REGION}:${ACCOUNT_ID}:cluster/truck-sim-cluster",
  "RoleArn": "arn:aws:iam::${ACCOUNT_ID}:role/ecsEventsRole",
  "EcsParameters": {
    "TaskDefinitionArn": "${TASK_DEF_ARN}",
    "LaunchType": "FARGATE",
    "NetworkConfiguration": {
      "awsvpcConfiguration": {
        "Subnets": ["${PUBLIC_SUBNET_ID}"],
        "SecurityGroups": ["${SECURITY_GROUP_ID}"],
        "AssignPublicIp": "ENABLED"
      }
    },
    "PlatformVersion": "LATEST"
  }
}
JSON

aws scheduler update-schedule \
     --name "$SCHED_NAME" \
     --schedule-expression "$CRON_EXPR" \
     --flexible-time-window '{"Mode":"OFF"}' \
     --target "$TARGET_JSON" \
     --region "$REGION" 2>/dev/null ||
aws scheduler create-schedule \
     --name "$SCHED_NAME" \
     --schedule-expression "$CRON_EXPR" \
     --flexible-time-window '{"Mode":"OFF"}' \
     --target "$TARGET_JSON" \
     --region "$REGION"

echo -e "\n✅  Scheduler '$SCHED_NAME' now launches the task in subnet ${PUBLIC_SUBNET_ID}"
echo "   First run occurs at the next 05:00 UTC."
echo "   Logs: CloudWatch → /ecs/truck-sim"

