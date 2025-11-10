#!/bin/bash
echo "Waiting for LocalStack..."
sleep 5
awslocal sqs create-queue --queue-name bike-trips
awslocal s3 mb s3://city-data-25
echo "LocalStack setup complete!"
