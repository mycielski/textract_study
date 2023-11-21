"""
This module provides functionality to process invoice receipts,
extract information using Amazon Textract, and compile a report from the extracted data.
It supports uploading and analyzing documents stored in a local directory to an AWS S3 bucket,
starting and monitoring Textract analysis jobs, and summarizing the results into a pandas DataFrame
which is then exported to an Excel file.
"""


import logging
import os
from pathlib import Path
from uuid import uuid4

import boto3
import pandas as pd
from tqdm import tqdm

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

BUCKET_NAME = os.getenv("AWS_BUCKET")
REGION_NAME = os.getenv("AWS_REGION")

NORMALIZED_FIELDS = [
    "INVOICE_RECEIPT_DATE",
    "INVOICE_RECEIPT_ID",
    "TAX_PAYER_ID",
    "CUSTOMER_NUMBER",
    "ACCOUNT_NUMBER",
    "VENDOR_NAME",
    "RECEIVER_NAME",
    "VENDOR_ADDRESS",
    "RECEIVER_ADDRESS",
    "ORDER_DATE",
    "DUE_DATE",
    "DELIVERY_DATE",
    "PO_NUMBER",
    "PAYMENT_TERMS",
    "TOTAL",
    "AMOUNT_DUE",
    "AMOUNT_PAID",
    "SUBTOTAL",
    "TAX",
    "SERVICE_CHARGE",
    "GRATUITY",
    "PRIOR_BALANCE",
    "DISCOUNT",
    "SHIPPING_HANDLING_CHARGE",
    "VENDOR_ABN_NUMBER",
    "VENDOR_GST_NUMBER",
    "VENDOR_PAN_NUMBER",
    "VENDOR_VAT_NUMBER",
    "RECEIVER_ABN_NUMBER",
    "RECEIVER_GST_NUMBER",
    "RECEIVER_PAN_NUMBER",
    "RECEIVER_VAT_NUMBER",
    "VENDOR_PHONE",
    "RECEIVER_PHONE",
    "VENDOR_URL",
    "ITEM",
    "QUANTITY",
    "PRICE",
    "UNIT_PRICE",
    "PRODUCT_CODE",
    "ADDRESS",
    "NAME",
    "ADDRESS_BLOCK",
    "STREET",
    "CITY",
    "STATE",
    "COUNTRY",
    "ZIP_CODE",
]


def upload_file_to_s3(local_file_path: Path, s3_key: str, prefix: str) -> str:
    """
    Upload a file to an S3 bucket and return its ARN.

    :param prefix: prefix for the s3 key
    :param s3_key: The key where the file will be saved on S3. Defaults to a random UUID.
    :param local_file_path: The local path to the file.
    :return: The key of the uploaded file.
    """
    # Initialize a session using Amazon S3 credentials
    s3_client = boto3.client("s3", region_name=REGION_NAME)

    s3_key = f"{prefix}/{s3_key}"

    # Upload the file
    logging.debug(f"Uploading {local_file_path} to {BUCKET_NAME}/{s3_key}")
    s3_client.upload_file(str(local_file_path), BUCKET_NAME, s3_key)
    logging.debug(f"Uploaded {local_file_path} to {BUCKET_NAME}/{s3_key}")

    return s3_key


def enumerate_directory(dir_path: Path, relevant_extensions: list[str]) -> list[Path]:
    """
    Enumerate all files in a given directory with a specific extension.

    :param dir_path: Path to the directory to search.
    :param relevant_extensions: The file extensions to look for.
    :return: A list of Path objects to the files with the relevant extension.
    """
    # check if dir_path exists and is a directory
    if not dir_path.is_dir():
        raise NotADirectoryError(f"{dir_path} is not a valid directory")

    # List to store all matching files
    relevant_files = []

    logging.info(
        f"Searching for files with extensions: {relevant_extensions} in {dir_path}"
    )

    for extension in relevant_extensions:
        # Recursively iterate over all files in the directory
        for file in dir_path.rglob(f"*.{extension}"):
            relevant_files.append(file)

    logging.info(f"Found {len(relevant_files)} files")
    return relevant_files


def upload_files(files: list[Path], prefix: str) -> list[str]:
    """
    Upload multiple files to an AWS S3 bucket.

    :param files: A list of Paths to the files to be uploaded.
    :param prefix: A prefix to prepend to the file names for organizational purposes.
    :return: A list of the full S3 keys for the uploaded files.
    """
    keys = []
    logging.info(f"Starting upload of {len(files)} files to {BUCKET_NAME}/{prefix}")
    for file in tqdm(files):
        keys.append(
            upload_file_to_s3(prefix=prefix, local_file_path=file, s3_key=file.name)
        )
    logging.info(f"Uploaded {len(keys)} files to {BUCKET_NAME}/{prefix}")
    return keys


def analyse_document(key: str):
    """
    Analyze a document using Amazon Textract.

    :param key: The S3 key of the document to analyze.
    :return: The response from the Textract client.
    """
    logging.debug(f"Starting analysis of {key}")
    textract_client = boto3.client("textract", region_name=REGION_NAME)
    response = textract_client.analyze_expense(
        Document={"S3Object": {"Bucket": BUCKET_NAME, "Name": key}}
    )
    logging.debug(f"Finished analysis of {key}")
    return response


def start_document_analyses(keys: list[str]) -> list[str]:
    """
    Start the analysis process for multiple documents using Amazon Textract.

    :param keys: A list of S3 keys representing the documents to be analyzed.
    :return: A list of job IDs for the initiated Textract jobs.
    """
    job_ids = []
    logging.info(f"Starting analysis of {len(keys)} files")
    for key in tqdm(keys):
        job_ids.append(start_document_analysis(key=key))
    logging.info(f"{len(job_ids)} jobs started")
    return job_ids


def start_document_analysis(key: str) -> str:
    """
    Start a Textract analysis job for a single document.

    :param key: The S3 key of the document to analyze.
    :return: The job ID for the Textract analysis job.
    """
    logging.debug(f"Starting analysis job for {key}")
    textract_client = boto3.client("textract", region_name=REGION_NAME)
    response = textract_client.start_expense_analysis(
        DocumentLocation={"S3Object": {"Bucket": BUCKET_NAME, "Name": key}}
    )
    logging.debug(f"Analysis job for {key} started. Job ID: {response['JobId']}")
    return response["JobId"]


def analyse_documents(keys: list[str]):
    """
    Analyze multiple documents and return their Textract responses.

    :param keys: A list of S3 keys for the documents to be analyzed.
    :return: A list of responses from the Textract client.
    """
    responses = []
    logging.info(f"Starting analysis of {len(keys)} files")
    for key in tqdm(keys):
        responses.append(analyse_document(key=key))
    logging.info(f"Finished analysis of {len(keys)} files")
    return responses


def summarize_textraction(textract_response: dict) -> dict:
    """
    Summarize a Textract response into a dictionary containing key information.

    :param textract_response: The response from the Textract service.
    :return: A dictionary with the summarized information.
    """
    summary_fields = textract_response["ExpenseDocuments"][0]["SummaryFields"]
    output = {}
    for field in summary_fields:
        field_type = field["Type"]["Text"]
        field_value = field["ValueDetection"]["Text"]
        if field_type in NORMALIZED_FIELDS:
            output[field_type] = field_value

    return output


def compile_report(textract_responses: list[dict]) -> pd.DataFrame:
    """
    Compile a report from a list of Textract responses into a pandas DataFrame.

    :param textract_responses: A list of dictionaries containing Textract responses.
    :return: A pandas DataFrame representing the compiled report.
    """
    logging.info(f"Compiling report from {len(textract_responses)} textract responses")
    # create a new df with columns from NORMALIZED_FIELDS
    report = pd.DataFrame(columns=NORMALIZED_FIELDS)

    for response in textract_responses:
        # create dict from NORMALIZED_FIELDS
        new_row_dict = {field: None for field in NORMALIZED_FIELDS}
        new_row_dict.update(summarize_textraction(textract_response=response))
        new_row_df = pd.DataFrame([new_row_dict])
        report = pd.concat([report, new_row_df], ignore_index=True)

    logging.info("Report compilation finished")

    return report


def retrieve_analyses(job_ids: list[str]) -> list[dict]:
    """
    Retrieve the analysis results for a list of Textract job IDs.

    :param job_ids: A list of job IDs for which to retrieve the results.
    :return: A list of dictionaries containing the analysis results.
    """
    logging.info(f"Retrieving analyses for {len(job_ids)} jobs")
    textract_client = boto3.client("textract", region_name=REGION_NAME)
    responses = []
    total_jobs = len(job_ids)

    progress_bar = tqdm(total=total_jobs)
    while job_ids:
        job_id = job_ids.pop()
        response = textract_client.get_expense_analysis(JobId=job_id)
        match response["JobStatus"]:
            case "SUCCEEDED":
                logging.debug(f"Job {job_id} succeeded")
                responses.append(response)
                progress_bar.update(1)
            case "IN_PROGRESS":
                logging.debug(f"Job {job_id} still in progress")
                job_ids.append(job_id)
            case "FAILED":
                logging.warning(f"Job {job_id} failed")
                progress_bar.update(1)
            case "PARTIAL_SUCCESS":
                logging.warning(f"Job {job_id} partially succeeded")
                responses.append(response)
                progress_bar.update(1)
            case _:
                logging.warning(f"Job {job_id} in unknown state")

    progress_bar.close()
    logging.info(f"Retrieved analyses for {total_jobs} jobs")
    return responses


def prettify_report(report: pd.DataFrame) -> pd.DataFrame:
    """
    This function takes a pandas DataFrame and drops columns where all values are NaN or None.

    :param report: A pandas DataFrame.
    :return: A pandas DataFrame with columns with values dropped.
    """
    logging.info("Prettifying the report")
    # Drop columns where all values are NaN
    logging.debug("Dropping columns where each row's value is empty")
    report_pretty = report.dropna(axis=1, how="all")
    logging.info("Prettified the report")
    return report_pretty


def main():
    """
    The main execution function for the script. Orchestrates the process of uploading files,
    initiating analysis jobs, compiling results, and exporting a report.
    """
    logging.info("Starting...")
    job_id = str(uuid4())
    logging.info(f"Job ID: {job_id}")
    files = enumerate_directory(
        dir_path=Path("demo_data"), relevant_extensions=["png", "jpg", "pdf"]
    )
    keys = upload_files(files=files, prefix=job_id)
    job_ids = start_document_analyses(keys=keys)
    responses = retrieve_analyses(job_ids=job_ids)
    report = compile_report(responses)
    report = prettify_report(report)
    report.to_excel(f"{job_id}.xlsx", index=False)


if __name__ == "__main__":
    main()
