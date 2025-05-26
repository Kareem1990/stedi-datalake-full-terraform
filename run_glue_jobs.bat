@echo off
echo Running customer_landing_to_trusted...
aws glue start-job-run --job-name customer_landing_to_trusted --region us-east-1 --output text
ping 127.0.0.1 -n 90 > nul

echo Running accelerometer_landing_to_trusted...
aws glue start-job-run --job-name accelerometer_landing_to_trusted --region us-east-1 --output text
ping 127.0.0.1 -n 90 > nul

echo Running step_trainer_trusted...
aws glue start-job-run --job-name step_trainer_trusted --region us-east-1 --output text
ping 127.0.0.1 -n 90 > nul

echo Running machine_learning_curated...
aws glue start-job-run --job-name machine_learning_curated --region us-east-1 --output text
