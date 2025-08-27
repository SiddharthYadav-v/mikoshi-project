import json
import os
import pickle
from typing import Any, Dict, List, Optional

import boto3
from aiobotocore.session import get_session
from botocore.exceptions import ClientError


class S3Utils:
    """
    A utility class for interacting with Amazon S3.

    Args:
        bucket_name (str): The name of the S3 bucket.
        aws_region (str): The AWS region where the S3 bucket is located.

    Attributes:
        s3 (boto3.client): The S3 client object.
        bucket_name (str): The name of the S3 bucket.

    """

    def __init__(self, bucket_name: str) -> None:
        """
        Initialize the S3Utils class.

        Args:
            bucket_name (str): The name of the S3 bucket.
        """
        self.s3 = boto3.client("s3")
        self.bucket_name = bucket_name

    def list_files_with_ext(
        self, prefix: str, extensions: Optional[List[str]] = None
    ) -> List[str]:
        """
        List files in the S3 bucket with the given prefix and matching the specified extensions.

        Args:
            prefix (str): The prefix to filter the S3 objects.
            extensions (List[str]): The list of file extensions to filter the S3 objects.

        Returns:
            List[str]: A list of file names matching the prefix and extensions.

        """
        paginator = self.s3.get_paginator("list_objects_v2")
        output_files = []
        for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
            if not extensions or len(extensions) == 0:
                output_files += [f["Key"] for f in page.get("Contents", [])]
            else:
                output_files += [
                    f["Key"]
                    for f in page.get("Contents", [])
                    if any(f["Key"].lower().endswith(ext.lower()) for ext in extensions)
                ]

        return output_files

    def download_all(
        self, local_download_path: str, keys_to_download: List[str]
    ) -> List[str]:
        """
        Downloads all the specified keys from the given bucket to the local download path.

        Args:
            bucket_name (str): The name of the bucket.
            local_download_path (str): The local path where the files will be downloaded.
            keys_to_download (list): A list of keys to download from the bucket.

        Returns:
            dict: A dictionary mapping the downloaded file locations to their corresponding keys.
        """
        download_loc = {}
        for key in keys_to_download:
            location = self.download_object(local_download_path, key)
            download_loc[key] = str(location)

        return download_loc

    def download_object(self, local_download_path: str, file_name: str) -> str:
        """Downloads an object from S3 to local.

        Args:
            local_download_path (str): The local path to download the object to.
            file_name (str): The name of the object to download.

        Returns:
            str: The path to the downloaded object.
        """
        os.makedirs(local_download_path, exist_ok=True)

        local_file_name = file_name.split("/")[-1]
        download_path = os.path.join(local_download_path, local_file_name)
        self.s3.download_file(self.bucket_name, file_name, str(download_path))
        return download_path

    def file_exists_in_s3(self, key: str) -> bool:
        """Checks if a file exists in S3.

        Args:
            key (str): The key of the object to check.

        Returns:
            bool: True if the object exists, False otherwise.

        Raises:
            ClientError: If an unexpected error occurs.
        """
        try:
            self.s3.head_object(Bucket=self.bucket_name, Key=key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            else:
                raise e  # Raise any other unexpected errors

    def load_file_from_s3(self, file_name: str) -> bytes:
        """
        Load a file from the S3 bucket.

        Args:
            file_name (str): The name of the file to load.

        Returns:
            bytes: The content of the file as bytes.

        """
        response = self.s3.get_object(Bucket=self.bucket_name, Key=file_name)
        file_content = response["Body"].read()
        return file_content

    def load_pickle_from_s3(self, file_name: str) -> Any:
        """
        Load a pickle object from the S3 bucket.

        Args:
            file_name (str): The name of the pickle file to load.

        Returns:
            Any: The deserialized pickle object.

        """
        return pickle.loads(self.load_file_from_s3(file_name))

    def load_text_from_s3(self, file_name: str) -> str:
        """
        Load a text file from the S3 bucket.

        Args:
            file_name (str): The name of the text file to load.

        Returns:
            str: The content of the text file as a string.

        """
        return self.load_file_from_s3(file_name).decode("utf-8")

    def load_json_from_s3(self, file_name: str) -> Dict:
        """
        Load a JSON file from the S3 bucket.

        Args:
            file_name (str): The name of the JSON file to load.

        Returns:
            Dict: The deserialized JSON object.

        """
        return json.loads(self.load_file_from_s3(file_name).decode("utf-8"))

    def copy_local_dir_to_s3(
        self, src_dir: str, dest_prefix: str, extensions: Optional[List[str]] = None
    ) -> None:
        """
        Copy files from a local directory to the S3 bucket.

        Args:
            src_dir (str): The path to the source directory.
            dest_prefix (str): The prefix to use for the destination S3 objects.
            extensions (Optional[List[str]]): The list of file extensions to filter the files.

        Returns:
            None
        """
        tgt_files = [os.path.join(dp, f) for dp, dn, fn in os.walk(src_dir) for f in fn]
        if extensions is not None and len(extensions) > 0:
            tgt_files = [
                f for f in tgt_files if any(f.endswith(ext) for ext in extensions)
            ]
        else:
            tgt_files = [f for f in tgt_files]
        tgt_files = [f.replace(f"{src_dir}/", "") for f in tgt_files]
        uploaded_files = {}
        for tgt_file in tgt_files:
            self.s3.upload_file(
                os.path.join(src_dir, tgt_file),
                self.bucket_name,
                os.path.join(dest_prefix, tgt_file),
            )
            uploaded_files[os.path.join(src_dir, tgt_file)] = (
                f"s3://{self.bucket_name}/{os.path.join(dest_prefix, tgt_file)}"
            )
        return uploaded_files

    def copy_s3_dir_to_local(
        self, src_prefix: str, dest_dir: str, extensions: Optional[List[str]] = None
    ) -> None:
        """
        Copy files from the S3 bucket to a local directory.

        Args:
            src_prefix (str): The prefix to filter the S3 objects.
            dest_dir (str): The path to the destination directory.
            extensions (Optional[List[str]]): The list of file extensions to filter the files. If None, all files will be copied.

        Returns:
            None
        """
        src_prefix = src_prefix.strip("/")
        s3_file_keys = self.list_files_with_ext(src_prefix, extensions)
        os.makedirs(dest_dir, exist_ok=True)
        for s3_file_key in s3_file_keys:
            print(f"Downloading {s3_file_key} to {dest_dir}")
            s3_file_name = s3_file_key.replace(f"{src_prefix}/", "")
            file_name = s3_file_name.split("/")[-1]
            folder_name = s3_file_name.replace(f"{file_name}", "")
            os.makedirs(os.path.join(dest_dir, folder_name), exist_ok=True)
            self.s3.download_file(
                self.bucket_name,
                os.path.join(src_prefix, s3_file_name),
                os.path.join(dest_dir, s3_file_name),
            )

    def copy_s3_to_s3(
        self, src_prefix: str, dest_prefix: str, extensions: Optional[List[str]] = None
    ) -> None:
        """
        Copy files from one S3 location to another within the same bucket.

        Args:
            src_prefix (str): The prefix to filter the source S3 objects.
            dest_prefix (str): The prefix to use for the destination S3 objects.
            extensions (Optional[List[str]]): The list of file extensions to filter the files. If None, all files will be copied.

        Returns:
            None
        """
        s3_file_keys = self.list_files_with_ext(src_prefix, extensions)

        for s3_file_key in s3_file_keys:
            copy_source = {"Bucket": self.bucket_name, "Key": s3_file_key}
            dest_key = s3_file_key.replace(
                src_prefix, dest_prefix, 1
            )  # Replace the source prefix with the destination prefix
            print(f"Copying {s3_file_key} to {dest_key}")
            self.s3.copy(copy_source, self.bucket_name, dest_key)

    def delete_s3_folder(self, prefix: str) -> None:
        """
        Delete a folder in the S3 bucket.

        Args:
            prefix (str): The prefix of the folder to delete.

        Returns:
            None
        """
        try:
            contents = self.s3.list_objects(Bucket=self.bucket_name, Prefix=prefix)[
                "Contents"
            ]
        except KeyError:
            contents = []
        for obj in contents:
            self.s3.delete_object(Bucket=self.bucket_name, Key=obj["Key"])

    def upload_file_to_s3(self, key: str, local_file_path: str) -> bool:
        """
        Upload a file to the S3 bucket.

        Args:
            key (str): The key to use for the S3 object.
            local_file_path (str): The bytes of the file to upload.

        Returns:
            None

        Raises:
            FileNotFoundError: If the file does not exist.
        """
        try:
            self.s3.upload_file(
                Bucket=self.bucket_name, Key=key, Filename=local_file_path
            )
            return True
        except FileNotFoundError:
            print("The file was not found.")
            return False

    def upload_fileobj_to_s3(self, key: str, fileobj: bytes) -> None:
        """
        Upload a file object to the S3 bucket.

        Args:
            key (str): The key to use for the S3 object.
            fileobj (bytes): The file object to upload.

        Returns:
            None
        """
        self.s3.put_object(Body=fileobj, Bucket=self.bucket_name, Key=key)

    def upload_string_to_s3(self, key: str, s: str) -> None:
        """
        Upload a string to the S3 bucket.

        Args:
            key (str): The key to use for the S3 object.
            s (str): The string to upload.

        Returns:
            None
        """
        self.s3.put_object(Bucket=self.bucket_name, Key=key, Body=s.encode("utf-8"))

    def upload_pickle_to_s3(self, key: str, obj: Any) -> None:
        """
        Upload a pickle object to the S3 bucket.

        Args:
            key (str): The key to use for the S3 object.
            obj (Any): The object to pickle and upload.

        Returns:
            None
        """
        self.s3.put_object(Bucket=self.bucket_name, Key=key, Body=pickle.dumps(obj))

    def upload_json_to_s3(self, key: str, obj: Dict) -> None:
        """
        Upload a JSON object to the S3 bucket.

        Args:
            key (str): The key to use for the S3 object.
            obj (Dict): The JSON object to upload.

        Returns:
            None
        """
        self.s3.put_object(Bucket=self.bucket_name, Key=key, Body=json.dumps(obj))

    def delete_specific_file(self, file_key: str):
        """
        Delete a files with file_key from the S3 bucket

        Args:
            file_key (str) : The key of the file that needs to be deleted

        Returns:
            None
        """
        self.s3.delete_object(Bucket=self.bucket_name, Key=file_key)

    async def upload_file_async(self, img_local_lctn: str, s3_key: str) -> None:
        """
        Upload a file asynchronously to the S3 bucket.

        Args:
            img_local_lctn (str): The local file path of the image to upload.
            s3_bucket (str): The name of the S3 bucket to upload to.
            s3_key (str): The key to use for the S3 object.

        Returns:
            None
        """
        session = get_session()
        async with session.create_client(
            "s3",
            region_name=os.getenv("AWS_REGION"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        ) as s3:
            with open(img_local_lctn, "rb") as file:
                await s3.put_object(Bucket=self.bucket_name, Key=s3_key, Body=file)

    @staticmethod
    async def upload_string_to_s3_async(s3_bucket: str, s3_key: str, file: str) -> None:
        """
        Upload a string as a file asynchronously to the S3 bucket.

        Args:
            s3_bucket (str): The name of the S3 bucket to upload to.
            s3_key (str): The key to use for the S3 object.
            file (str): The string content to upload.

        Returns:
            None
        """
        session = get_session()
        async with session.create_client(
            "s3",
            region_name=os.getenv("AWS_REGION"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        ) as s3:
            await s3.put_object(Bucket=s3_bucket, Key=s3_key, Body=file.encode("utf-8"))

    async def upload_fileobj_to_s3_async(self, key: str, fileobj: bytes) -> None:
        """
        Upload a file object asynchronously to the S3 bucket.

        Args:
            key (str): The key to use for the S3 object.
            fileobj (bytes): The file object to upload.

        Returns:
            None
        """
        session = get_session()
        async with session.create_client(
            "s3",
            region_name=os.getenv("AWS_REGION"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        ) as s3:
            await s3.put_object(Bucket=self.bucket_name, Key=key, Body=fileobj)

    async def upload_json_to_s3_async(self, document: Dict, s3_key: str) -> None:
        """
        Upload a JSON document asynchronously to the S3 bucket.

        Args:
            document (dict): The JSON document to upload.
            s3_key (str): The key to use for the S3 object in S3.

        Returns:
            None
        """
        session = get_session()
        async with session.create_client(
            "s3",
            region_name=os.getenv("AWS_REGION"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        ) as s3:
            json_data = json.dumps(document)
            await s3.put_object(Bucket=self.bucket_name, Key=s3_key, Body=json_data)

    @staticmethod
    def parse_s3_key(s3_path: str) -> str:
        """
        Parse the S3 key from an S3 path.

        Args:
            s3_path (str): The full S3 path in the format 's3://bucket-name/key'.

        Returns:
            str: The extracted S3 key.
        """
        # Remove the 's3://' prefix
        path_without_scheme = s3_path[5:]
        # Find the first '/' after the bucket name and extract the key
        key_start_index = path_without_scheme.find("/") + 1
        key = path_without_scheme[key_start_index:]
        return key


def download_files_from_s3(args, s3_utils: S3Utils, temp_dir: str) -> dict[str, str]:
    """
    Download S3 files to local directory and return local paths.
    
    Args:
        args: CLI arguments containing file paths
        s3_utils: S3Utils instance for downloads
        temp_dir: Temporary directory for downloads
        
    Returns:
        Dictionary with local file paths
    """
    from .file_utils import is_s3_path
    
    def download_if_s3(s3_path: str, name: str) -> str:
        if is_s3_path(s3_path):
            print(f"Downloading {name} from S3...")
            bucket, key = s3_path[5:].split("/", 1)
            if bucket != s3_utils.bucket_name:
                temp_s3_utils = S3Utils(bucket)
                return temp_s3_utils.download_object(temp_dir, key)
            else:
                return s3_utils.download_object(temp_dir, key)
        return s3_path

    return {
        'pdf_path': download_if_s3(args.pdf, "PDF"),
        'markdown_path': download_if_s3(args.markdown, "Markdown"), 
        'template_raw_path': args.template_raw,  # Keep S3 path for PDF highlighter
        'template_cleaned_path': args.template_cleaned  # Keep S3 path for template processor
    }


def upload_comparison_results_to_s3(report, config: dict, s3_utils: S3Utils, 
                                   transaction_id: str, transaction_output_dir: str) -> None:
    """
    Upload comparison results to S3.
    
    Args:
        report: Comparison report to upload
        config: Configuration dictionary
        s3_utils: S3Utils instance for uploads
        transaction_id: Transaction identifier
        transaction_output_dir: Local output directory path
    """
    import os
    
    print("Uploading results to S3...")
    s3_output_prefix = config['s3']['output_prefix'].format(transaction_id=transaction_id)

    # Upload main report
    report_s3_key = f"{s3_output_prefix}comparison_report.json"
    s3_utils.upload_string_to_s3(report_s3_key, report.to_json())
    print(f"Report uploaded to: s3://{s3_utils.bucket_name}/{report_s3_key}")

    # Upload intermediate files if output directory exists
    if os.path.exists(transaction_output_dir):
        for root, dirs, files in os.walk(transaction_output_dir):
            for file in files:
                local_file_path = os.path.join(root, file)
                relative_path = os.path.relpath(local_file_path, transaction_output_dir)
                s3_key = f"{s3_output_prefix}{relative_path}"
                s3_utils.upload_file_to_s3(s3_key, local_file_path)
