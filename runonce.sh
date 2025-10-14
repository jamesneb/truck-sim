aws ecs run-task \
  --cluster truck-sim-cluster \
  --task-definition truck-sim \                 # latest revision automatically picked
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[subnet-0faf5b311289ea0af],securityGroups=[sg-03b208d4bb055a8c9],assignPublicIp=ENABLED}" \
  --region us-east-1

