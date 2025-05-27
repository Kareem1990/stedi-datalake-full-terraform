#!/bin/bash

echo "⏳ Waiting 15 seconds for Glue catalog to be ready..."
sleep 15

echo "▶️ Running customer_landing_to_trusted..."
aws glue start-job-run --job-name customer_landing_to_trusted --output text

echo "▶️ Running accelerometer_landing_to_trusted..."
aws glue start-job-run --job-name accelerometer_landing_to_trusted --output text

echo "▶️ Running step_trainer_trusted..."
aws glue start-job-run --job-name step_trainer_trusted --output text

echo "▶️ Running machine_learning_curated..."
aws glue start-job-run --job-name machine_learning_curated --output text
