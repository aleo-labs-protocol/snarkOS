// Copyright (c) 2019-2026 Provable Inc.
// This file is part of the snarkOS library.

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:

// http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::permissions::Permission;

use anyhow::Result;
use hyper::Client as HyperClient;
use hyper_tls::HttpsConnector;
use rusoto_core::{
    Region,
    RusotoError,
    credential::{AwsCredentials, StaticProvider},
    request::HttpClient,
};
use rusoto_s3::{
    CreateBucketRequest,
    Delete,
    DeleteBucketRequest,
    DeleteObjectOutput,
    DeleteObjectRequest,
    DeleteObjectsRequest,
    GetObjectError,
    GetObjectOutput,
    GetObjectRequest,
    ListBucketsOutput,
    ListObjectsOutput,
    ListObjectsRequest,
    ListObjectsV2Output,
    ListObjectsV2Request,
    ObjectIdentifier,
    PutObjectOutput,
    PutObjectRequest,
    S3,
    S3Client,
};

/// A client to access a storage bucket.
#[derive(Clone)]
pub struct Client {
    region: String,
    endpoint: String,
    s3: S3Client,
}

impl Client {
    /// Initializes a new instance of a client.
    pub fn new(region: String, endpoint: String, key: String, secret: String, token: Option<String>) -> Self {
        let s3 = {
            let hyper_builder = HyperClient::builder();
            // Uncomment this one if retries happen too often:
            // hyper_builder.pool_idle_timeout(Duration::from_secs(10));

            // Uncomment this one if there are still retry-related panics.
            // hyper_builder.max_idle_per_host(0);

            let dispatcher = HttpClient::from_builder(hyper_builder, HttpsConnector::new());

            let credentials = StaticProvider::from(AwsCredentials::new(key, secret, token, None));
            let custom_region = Region::Custom { name: region.clone(), endpoint: endpoint.clone() };

            S3Client::new_with(dispatcher, credentials, custom_region)
        };

        Self { region, endpoint, s3 }
    }

    pub fn region(&self) -> &str {
        &self.region
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn update_region(&mut self, region: String) {
        self.region = region;
    }

    pub fn s3(&self) -> &S3Client {
        &self.s3
    }
}

impl Client {
    /// Returns a list of files in the given bucket.
    pub async fn list_files(
        &self,
        bucket: String,
        prefix: Option<String>,
        marker: Option<String>,
    ) -> Result<ListObjectsOutput> {
        let list_objects_request = ListObjectsRequest { bucket, prefix, marker, ..Default::default() };

        Ok(self.s3().list_objects(list_objects_request).await?)
    }

    /// Returns a list of objects in the given bucket starting from the given key.
    pub async fn list_objects_after_key(
        &self,
        bucket: String,
        key: String,
        continuation_token: Option<String>,
    ) -> Result<ListObjectsV2Output> {
        let list_objects_request =
            ListObjectsV2Request { bucket, start_after: Some(key), continuation_token, ..Default::default() };

        Ok(self.s3().list_objects_v2(list_objects_request).await?)
    }

    /// Creates the given file in the bucket.
    pub async fn create_file(
        &self,
        bucket: String,
        file_name: String,
        acl: Permission,
        body: Option<Vec<u8>>,
    ) -> Result<PutObjectOutput> {
        let put_object_request = PutObjectRequest {
            acl: Some(acl.to_string()),
            bucket,
            key: file_name,
            body: body.map(|data| data.into()),
            cache_control: Some("no-cache, max-age=0".to_string()),
            ..Default::default()
        };

        Ok(self.s3().put_object(put_object_request).await?)
    }

    /// Returns the file in the given bucket with the given file name.
    pub async fn get_file(
        &self,
        bucket: String,
        file_name: String,
    ) -> Result<GetObjectOutput, RusotoError<GetObjectError>> {
        let get_object_req = GetObjectRequest { bucket, key: file_name, ..Default::default() };

        self.s3().get_object(get_object_req).await
    }

    /// Removes the given file from the bucket.
    pub async fn delete_file(&self, bucket: String, file_name: String) -> Result<DeleteObjectOutput> {
        let delete_object_request = DeleteObjectRequest { bucket, key: file_name, ..Default::default() };

        Ok(self.s3().delete_object(delete_object_request).await?)
    }
}

// Unused methods.
impl Client {
    pub async fn list_buckets(&self) -> Result<ListBucketsOutput> {
        Ok(self.s3().list_buckets().await?)
    }

    pub async fn bucket_exists(&self, bucket: String) -> bool {
        self.list_files(bucket, None, None).await.is_ok()
    }

    /// Create a bucket with name `bucket` and return the location it was deployed
    pub async fn create_bucket(&self, bucket: String, acl: Permission) -> Result<Option<String>> {
        //        let create_bucket_configuration = Some(CreateBucketConfiguration {
        //            location_constraint: Some(self.region()),
        //        });

        let create_bucket_req = CreateBucketRequest { acl: Some(acl.to_string()), bucket, ..Default::default() };

        let output = self.s3().create_bucket(create_bucket_req).await?;

        Ok(output.location)
    }

    pub async fn delete_bucket(&self, bucket: String) -> Result<()> {
        let delete_bucket_req = DeleteBucketRequest { bucket, expected_bucket_owner: None };

        Ok(self.s3().delete_bucket(delete_bucket_req).await?)
    }

    pub async fn directory_exists(&self, bucket: String, directory: String) -> bool {
        match self.list_files(bucket, Some(directory), None).await {
            Ok(output) => match output.contents {
                Some(contents) => !contents.is_empty(),
                None => false,
            },
            _ => false,
        }
    }

    pub async fn create_directory(&self, bucket: String, directory: &str, acl: Permission) -> Result<PutObjectOutput> {
        let key = match directory.ends_with('/') {
            true => directory.to_owned(),
            false => format!("{}/", directory),
        };

        let put_directory_request = PutObjectRequest { acl: Some(acl.to_string()), bucket, key, ..Default::default() };

        Ok(self.s3().put_object(put_directory_request).await?)
    }

    pub async fn delete_directory(&self, bucket: String, directory: &str) -> Result<DeleteObjectOutput> {
        let key = match directory.ends_with('/') {
            true => directory.to_owned(),
            false => format!("{}/", directory),
        };

        let delete_directory_request =
            DeleteObjectRequest { bucket: bucket.clone(), key: key.clone(), ..Default::default() };

        let mut object_identifiers = vec![];

        if let Some(objects) = self.list_files(bucket.clone(), Some(key.clone()), None).await?.contents {
            for object in objects {
                if let Some(key) = object.key {
                    object_identifiers.push(ObjectIdentifier { key, ..Default::default() });
                }
            }
        }

        let delete_objects_request = DeleteObjectsRequest {
            bucket,
            delete: Delete { objects: object_identifiers, ..Default::default() },
            ..Default::default()
        };

        self.s3().delete_objects(delete_objects_request).await?;

        Ok(self.s3().delete_object(delete_directory_request).await?)
    }
}
