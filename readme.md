# AWS Textract study

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

Some code I've written when learning what is Textract and how to use it.

## How to use it?

1. Put your invoices in the `demo_data` directory.
   Here's an example of the directory structure:
    ```
    .
    ├── demo_data
    │   ├── invoice.pdf
    │   └── invoices
    │       ├── other_invoice.jpg
    │       └── and_one_more_invoice.png
    ├── readme.md
    └── src
        └── main.py
    ```

2. Provide your AWS credentials as environment variables:
    ```
    export AWS_ACCESS_KEY_ID=your_access_key_id          # for example "AKIAIOSFODNN7EXAMPLE"
    export AWS_SECRET_ACCESS_KEY=your_secret_access_key  # for example "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
    export AWS_REGION=region                             # for example "us-east-1"
    export AWS_BUCKET=bucket_name                        # for example "my-textract-study-bucket"
    ```

3. Run the script:
    ```shell
    $ python src/main.py
    ```

4. The report will be generated with a name like `<uuid>.xlsx` in the top directory:
    ```
    .
    ├── 456af71d-f7b2-4bf8-87c7-bade21d843d4.xlsx
    ├── demo_data
    │   ├── invoice.pdf
    │   └── invoices
    │       ├── other_invoice.jpg
    │       └── and_one_more_invoice.png
    ├── readme.md
    └── src
        └── main.py
    ```

## Notes

- This script uses busy waiting for Textract job results (in the `retrieve_analyses` function). It is not optimal. In fact, it is pretty terrible for performance. Use [notifications](https://docs.aws.amazon.com/textract/latest/dg/api-async.html#:~:text=The%20completion%20status%20of%20the%20request%20is%20published%20to%20an%20Amazon%20Simple%20Notification%20Service%20(Amazon%20SNS)%20topic.) instead.
- The whole thing is just one file. Terrible for legibility but eh, it works.
