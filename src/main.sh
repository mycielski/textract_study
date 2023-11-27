#!/bin/sh

export AWS_PAGER="cat"

job_uuid=$(uuidgen | tr "[:upper:]" "[:lower:]")
echo "$job_uuid"
workers=9

mkfifo /tmp/job_ids
trap "rm -f /tmp/job_ids" EXIT INT TERM HUP

mkfifo /tmp/s3_objects
trap "rm -f /tmp/s3_objects" EXIT INT TERM HUP

# shellcheck disable=SC2140
# shellcheck disable=SC2016
find "$1" -type f \( -name "*.png" -or -name "*.jpg" -or -name "*.jpeg" -or -name "*.pdf" \) -print0 |
xargs -0 -n 1 -P $workers -I {} aws s3 cp --no-progress "{}" s3://"$AWS_BUCKET"/"$job_uuid"/"{}" >> /tmp/s3_objects &

awk -F  "s3://$AWS_BUCKET/" '{print $2}' /tmp/s3_objects |
tr '\n' '\0' |
xargs -0 -I {} echo "'{\"S3Object\":{\"Bucket\":\"$AWS_BUCKET\",\"Name\":\"{}\"}}'" |
pv --rate-limit 64 --quiet |
xargs -n 1 -P $workers aws textract start-expense-analysis --document-location --region "$AWS_REGION" --no-cli-pager |
jq -r '.JobId' > /tmp/job_ids &

#mkfifo /tmp/responses
#trap "rm -f /tmp/responses" EXIT INT TERM HUP
#mkfifo /tmp/errors
#trap "rm -f /tmp/errors" EXIT INT TERM HUP
#
#while read -r job_id; do
#  echo "Job ID: $job_id"
#  response=$(aws textract get-expense-analysis --job-id "$job_id" --region "$AWS_REGION" --no-cli-pager)
#  job_status=$(echo "$response" | jq -r '.JobStatus')
#  if [ "$job_status" = "SUCCEEDED" ]; then
#    echo "$response" >> /tmp/responses
#  elif [ "$job_status" = "IN_PROGRESS" ]; then
#    echo "$response" >> /tmp/job_ids
#  elif [ "$job_status" = "PARTIAL_SUCCESS" ]; then
#    echo "$response" >> /tmp/responses
#  else
#    echo "$response" >> /tmp/errors
#  fi
#done < /tmp/job_ids
#
