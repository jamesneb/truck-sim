#!/usr/bin/env bash
set -euo pipefail

##############################################################################
# edit here only if you move to another Region or change SG IDs
REGION="us-east-1"
OS_SG="sg-025d4c6949b665fe7"    # OpenSearch domain security group
TASK_SG="sg-03b208d4bb055a8c9"  # Fargate task security group
##############################################################################

echo "🔍  Checking if rule already exists…"

if aws ec2 describe-security-groups \
      --group-ids "$OS_SG" \
      --region "$REGION" \
      --query "SecurityGroups[0].IpPermissions[?to_string(FromPort)==\`443\` && IpProtocol==\`tcp\`].UserIdGroupPairs[?GroupId==\`$TASK_SG\`]" \
      --output text | grep -q "$TASK_SG"; then
  echo "✅  Inbound TCP 443 from $TASK_SG is already allowed on $OS_SG"
  exit 0
fi

echo "➕  Authorizing inbound TCP 443 from $TASK_SG to $OS_SG …"

aws ec2 authorize-security-group-ingress \
     --group-id "$OS_SG" \
     --protocol tcp \
     --port 443 \
     --source-group "$TASK_SG" \
     --region "$REGION"

echo "✅  Rule added. OpenSearch is now reachable from the Fargate task."

