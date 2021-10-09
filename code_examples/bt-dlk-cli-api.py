import os
import json
import binascii
from jsonschema import validate
from multiprocessing.dummy import Pool
from datetime import datetime, timedelta

import requests
# from urllib.error import HTTPError

PROD_BASE_URL = "https://datalake.cloudtraxx.com"
CORE_DESINATION = "core"
POOL_COUNT = 20

class ApiClient(object):

    def __init__(self, username, password, base_url=None, pool_count=None):
        if base_url is None:
            self.base_url = PROD_BASE_URL
        else:
            self.base_url = base_url
        if pool_count is None or int(pool_count) > 50:
            self.pool_count = POOL_COUNT
        else:
            self.pool_count = int(pool_count)            
        self.username = username
        self.session = requests.Session()
        self.session.auth = (username, password)
        self.session.headers["Content-Type"] = "application/json"

    def is_gz_file(self, filepath):
        with open(filepath, 'rb') as test_f:
            return binascii.hexlify(test_f.read(2)) == b'1f8b'

    def get_user(self):
        response = self.session.get(self.base_url + "/user")
        return response
    
    def get_user_authorization(self, username, project):
        body = {
            "username": username,
            "project": project
        }
        response = self.session.put("{}/{}".format(self.base_url, "user/authorization"), json=body)
        return response

    def get_project(self, project):
        body = {
            "project": project
        }
        response = self.session.put("{}/{}".format(self.base_url, "project"), json=body)
        return response

    def get_projects(self):
        response = self.session.get("{}/{}".format(self.base_url, "projects"))
        return response

    def put_upload_stage(self, region, payload_path, businessobject):
        # Generate pre-signed S3 urls
        
        if not self.is_gz_file(payload_path):
            result =  {
                "result": {
                    "uploadsSuccessful" : 0,
                    "uploadsFailed" : {
                        "count": 1,
                        "files": payload_path,
                        "error": "File is not gzip compressed."
                    }
                }
            }
            return result

        body = {
            "region": region,
            "businessobject": businessobject,
            "payload": {
                "key": os.path.basename(payload_path),
                "fileSize": os.path.getsize(payload_path)
            }
        }
        
        response = self.session.put("{}/{}".format(self.base_url, "upload-stage"), json=body)

        # Check if we successfully generated a presigned urls
        response.raise_for_status()
        payload_presigned_url = response.json()["payloadPresignedUrl"]
        tags = response.json()["tags"]

        # Build arguments for upload_file function below
        files_to_upload = [
            {
                "file_path": payload_path,
                "presigned_url": payload_presigned_url,
                "tags": tags
            }
        ]

        # Multithreaded to simultaneously upload payload and metadata
        pool = Pool(1)
        responses = pool.map(self.upload_file, files_to_upload)
        pool.close()

        # Check uploads were successful
        for r in responses:
            r.raise_for_status()

        response = {
            "filesUploaded": {
                "payload": files_to_upload[0]["file_path"]
            }
        }

        return response


    def put_upload_core(self, region, payload_path, metadata_path):
        # Generate pre-signed S3 urls
        
        body = {
            "region": region,
            "payload": {
                "key": os.path.basename(payload_path),
                "fileSize": os.path.getsize(payload_path)
            },
            "metadata": {
                "key": os.path.basename(metadata_path),
                "fileSize": os.path.getsize(metadata_path)
            }
        }
        
        response = self.session.put("{}/{}".format(self.base_url, "upload-core"), json=body)

        # Check if we successfully generated a presigned urls
        response.raise_for_status()
        payload_presigned_url = response.json()["payloadPresignedUrl"]
        metadata_presigned_url = response.json()["metadataPresignedUrl"]
        tags = response.json()["tags"]

        # Build arguments for upload_file function below
        files_to_upload = [
            {
                "file_path": payload_path,
                "presigned_url": payload_presigned_url,
                "tags": tags
            },
            {
                "file_path": metadata_path,
                "presigned_url": metadata_presigned_url,
                "tags": tags
            },
        ]

        # Multithreaded to simultaneously upload payload and metadata
        pool = Pool(2)
        responses = pool.map(self.upload_file, files_to_upload)
        pool.close()

        # Check uploads were successful
        for r in responses:
            r.raise_for_status()

        response = {
            "filesUploaded": {
                "payload": files_to_upload[0]["file_path"],
                "metadata": files_to_upload[1]["file_path"]
            }
        }

        return response


    def put_upload_consumption(self, project, file_path, prefix=None):
        # Check if a prefix is specified
        if prefix:
            key = "{}/{}".format(prefix, os.path.basename(file_path))
        else:
            key = file_path
        # Generate pre-signed S3 urls
        body = {
            "project": project,
            "payload": {
                "key": key,
                "fileSize": os.path.getsize(file_path)
            }
        }
        response = self.session.put("{}/{}".format(self.base_url, "upload-consumption"), json=body)

        # Check if we successfully generated a presigned urls
        response.raise_for_status()
        presigned_url = response.json()["payloadPresignedUrl"]
        tags = response.json()["tags"]

        # Build arguments for upload_file function below
        file_to_upload = [
            {
                "file_path": file_path,
                "presigned_url": presigned_url,
                "tags": tags
            }
        ]
        # print (file_to_upload)
        # Multithreaded to simultaneously upload payload and metadata
        file_count = len(file_to_upload)
        pool = Pool(file_count)
        responses = pool.map(self.upload_file, file_to_upload)
        # print (responses)
        pool.close()

        # Check uploads were successful
        if not responses[0].status_code == 200:
            upload_success = False
        else:
            upload_success = True

        return upload_success


    def upload_file(self, file_to_upload):
        # PUT to a presigned URL
        file_path = file_to_upload["file_path"]
        presigned_url = file_to_upload["presigned_url"]
        tags = file_to_upload["tags"]
        with open(file_path, "rb") as f:
            response = requests.put(
                presigned_url,
                headers={
                    "x-amz-server-side-encryption": "aws:kms",
                    "x-amz-tagging": "Project={Project}&CostCenter={CostCenter}&Solution={Solution}&BusinessUnit={BusinessUnit}&BusinessObject={BusinessObject}".format(
                        **tags
                    )
                },
                data=f
            )
        return response


    def put_bulk_upload_core(self, files_to_upload):
        payload = files_to_upload["payload"]
        metadata = files_to_upload["metadata"]
        region = files_to_upload["region"]
        try:
            response = self.put_upload_core(
                region,
                payload,
                metadata
            )
            index = self.put_file_index(
                "core",
                region,
                "bis",
                response["filesUploaded"]["payload"],
                response["filesUploaded"]["metadata"]
            )
            return True
        except Exception as e:
            return {
                "error": e,
                "payload": payload,
                "metadata": metadata
            }


    def put_upload_manifest(self, manifestfile):
        with open(manifestfile) as json_data:
            manifest = json.load(json_data)
        
        file_count = len(manifest["objects"])
        failed_uploads_count = 0
        failed_uploads = []
        files_to_upload = []

        for item in manifest["objects"]:
            region = item["region"]
            payload = item["dataFile"]
            metadata = "{}.meta".format(os.path.splitext(payload)[0])
            if not os.path.isfile(payload) or not os.path.isfile(metadata):
                failed_uploads_count += 1
                failed_uploads.append(
                    {
                        "dataFile": payload,
                        "metadataFile": metadata,
                        "reason": "Data or metadata file not readable or does not exist"
                    }
                )
                continue
            files_to_upload.append(
                {
                    "region": region,
                    "payload": payload,
                    "metadata": metadata,
                }
            )
        # print(files_to_upload)
        # here we will pool and map for put_upload_core
        upload_count = len(files_to_upload)
        # pool = Pool(upload_count)
        pool = Pool(self.pool_count)
        
        responses = pool.map(self.put_bulk_upload_core, files_to_upload)
        pool.close()

        # Check uploads were successful
        for response in responses:
            if not response is True:
                failed_uploads_count += 1
                failed_uploads.append(
                    {
                        "dataFile": response["payload"],
                        "metadataFile": response["metadata"],
                        "reason": str(response["error"])
                    }
                )

        result =  {
            "result": {
                "uploadsSuccessful" : (file_count - failed_uploads_count),
                "uploadsFailed" : {
                    "count": failed_uploads_count,
                    "files": failed_uploads
                }
            }
        }
        return result
        
    def register_businessobject(self, manifestfile, updateflag=False):
        with open(manifestfile) as json_data:
            manifest = json.load(json_data)
        body = {
            "updateflag": updateflag,
            "manifest": manifest
        }
        result = self.session.put("{}/{}".format(self.base_url, "reg-bobject"), json=body)
        return result

    def list_businessobject(self, businessobject=None):
        if businessobject:
            body = {
                "businessobject": businessobject
            }
        else:
            body = {}
        result = self.session.put("{}/{}".format(self.base_url, "get-bobject"), json=body)
        return result            

    def download_file(self, file_to_download):
        # GET to a presigned URL
        file = file_to_download["file"]
        file = file.replace('/','_')
        # print (file)
        presigned_url = file_to_download["presigned_url"]
        
        response = requests.get(presigned_url)

        if response.status_code is 200:
            with open(file, 'wb') as f:
                f.write(response.content)
        return response


    def put_copy(self, project, payload_key):
        # Mark upload as successful
        body = {
            "project": project,
            "payload": {
                "key": payload_key
            }
        }
        response = self.session.put(self.base_url + "/copy", json=body)
        return response

    def put_bulk_copy_consumption(self, files_to_copy):
        payload = files_to_copy["payload"]
        metadata = files_to_copy["metadata"]
        project = files_to_copy["project"]
        try:
            response = self.put_copy(
                project, 
                payload,
            )
            if not isinstance(response.json(), bool):
                raise ValueError(response.text)
                # if 'message' in ascii(response).json():
                    # raise ValueError(ascii(response).json()["message"])            
                # if 'errorMessage' in ascii(response).json():
                    # raise ValueError(ascii(response).json()["errorMessage"])
            # response = self.put_copy(
                # project, 
                # metadata,
            # ) 
            # print(metadata)
            # print(response.json())
            # if not isinstance(response.json(), bool):
                # if 'errorMessage' in response.json():
                    # raise ValueError(response.json()["errorMessage"])
            return {
                "error": "NoError",
                "payload": payload,
                "metadata": metadata
            }    
        except ValueError as e:
            return {
                "error": str(response.text),
                "payload": payload,
                "metadata": metadata
            }        
        
        
    def copy_manifest(self, manifestfile):
        # with open('manifest.json') as json_data:
        with open(manifestfile) as json_data:
            manifest = json.load(json_data)

        file_count = len(manifest["objects"])
        failed_copy_count = 0
        failed_copy = []
        files_to_copy = []

        for item in manifest["objects"]:
            payload = item["dataFile"]
            project = item["project"]
            metadata = "{}.meta".format(os.path.splitext(payload)[0])

            files_to_copy.append(
                {
                    "payload": os.path.basename(payload),
                    "metadata": os.path.basename(metadata),
                    "project": project
                }
            )

        # here we will pool and map for put_upload_core
        copy_count = len(files_to_copy)
        # pool = Pool(copy_count)
        pool = Pool(self.pool_count)
        responses = pool.map(self.put_bulk_copy_consumption, files_to_copy)
        pool.close()

        # Check uploads were successful
        for response in responses:
            if str(response["error"]) != "NoError":
                failed_copy_count += 1
                failed_copy.append(
                    {
                        "dataFile": response["payload"],
                        "metadataFile": response["metadata"],
                        "reason": str(response["error"])
                    }
                )

        result =  {
            "result": {
                "copySuccessful" : (file_count - failed_copy_count),
                "copyFailed" : {
                    "count": failed_copy_count,
                    "files": failed_copy
                }
            }
        }
        return result
        

    def get_download_consumption(self, project, payloads):
        # Generate pre-signed S3 urls
        list_of_payloads = []
        for payload in payloads:
            list_of_payloads.append(
                {
                    "project": project,
                    "payload": payload
                }
            )


        def generate_download_url(body):
            return self.session.put("{}/{}".format(self.base_url, "download"), json=body)

        payload_count = len(list_of_payloads)
        pool = Pool(payload_count)
        responses = pool.map(generate_download_url, list_of_payloads)
        pool.close()

        presigned_urls = []
        for r in responses:
            r.raise_for_status()
            presigned_urls.append(r.json()["presignedUrl"])

        # Build arguments for download_file
        files_to_download = []
        for payload, presigned_url in zip(payloads, presigned_urls):
            files_to_download.append(
                {
                    "file": payload,
                    "presigned_url": presigned_url
                }
            )

        # Multithreaded to simultaneously download multiple payloads
        file_count = len(files_to_download)
        pool = Pool(file_count)
        responses = pool.map(self.download_file, files_to_download)
        pool.close()

        return responses


    def put_file_index(self, bucket, region, project, payload_key, metadata_key):
        # Mark upload as successful
        body = {
            "bucket": bucket,
            "region": region,
            "project": project,
            "payload": {
                "key": payload_key
            },
            "metadata": {
                "key": metadata_key
            }
        }
        response = self.session.put(self.base_url + "/file-index", json=body)
        return response


    def get_list_files(self, project=None, day_range=None):
        body = {}

        if project:
            body["project"] = project

        response = self.session.put("{}/{}".format(self.base_url, "list-files"), json=body)
        # print(response)
        response.raise_for_status()
        response_dict = response.json()
        # print(response_dict)

        if day_range:
            # have to convert back to datetime object to do delta
            response_dict = response.json()
            for p in response_dict["fileList"]:
                files = p["files"]
                for f in files:
                    f["lastModified"] = datetime.strptime(f["lastModified"], "%d/%m/%y %H:%M.%S")

                # do datetime delta
                time_delta = datetime.now() - timedelta(days=day_range)
                files = [f for f in files if f["lastModified"] >= time_delta]

                for f in files:
                    f["lastModified"] = f["lastModified"].strftime("%d/%m/%y %H:%M.%S")
                p["files"] = files
            # print file_list

        return response_dict
