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

use anyhow::{Result, bail};
use futures::TryStreamExt;
use object_store::{
    Attribute,
    Attributes,
    ObjectStore,
    ObjectStoreExt,
    PutOptions,
    PutPayload,
    aws::AmazonS3Builder,
    gcp::GoogleCloudStorageBuilder,
    path::Path,
};
use std::sync::Arc;

/// The `Cache-Control` header of every uploaded object.
///
/// HAZARD: the CDN client polls `latest.json` to learn the bucket's height, so a cache in front
/// of the bucket must not serve a stale copy of it.
const CACHE_CONTROL: &str = "no-cache, max-age=0";

#[derive(Debug, Clone)]
pub struct BucketFile {
    pub file_path: String,
    pub file_size: u64,
}

/// A storage bucket.
///
/// The bucket is expected to grant public read access itself; uploads set no per-object ACL.
#[derive(Debug, Clone)]
pub struct Bucket {
    store: Arc<dyn ObjectStore>,
    bucket_name: String,
}

impl Bucket {
    /// Initializes a bucket backed by the given object store.
    pub fn new(store: Arc<dyn ObjectStore>, bucket_name: impl Into<String>) -> Self {
        Self { store, bucket_name: bucket_name.into() }
    }

    /// Initializes a bucket from a URL: `gs://<bucket>` for Google Cloud Storage, or
    /// `s3://<bucket>` for an S3-compatible service.
    ///
    /// The credentials and other settings are read from the environment variables that the
    /// `object_store` builders read, such as `GOOGLE_SERVICE_ACCOUNT` or `AWS_ENDPOINT`.
    pub fn from_url(url: &str) -> Result<Self> {
        let store: Arc<dyn ObjectStore> = match url.split_once("://") {
            Some(("gs", _)) => Arc::new(GoogleCloudStorageBuilder::from_env().with_url(url).build()?),
            Some(("s3", _)) => Arc::new(AmazonS3Builder::from_env().with_url(url).build()?),
            _ => bail!("Unsupported bucket URL '{url}': expected 'gs://<bucket>' or 's3://<bucket>'"),
        };
        let bucket_name = url.split_once("://").map(|(_, rest)| rest.trim_end_matches('/')).unwrap_or_default();
        Ok(Self::new(store, bucket_name))
    }

    /// Returns the bucket name.
    pub fn name(&self) -> &str {
        &self.bucket_name
    }

    /// Returns a list of all files in the bucket.
    pub async fn list_files(&self) -> Result<Vec<BucketFile>> {
        let objects: Vec<_> = self.store.list(None).try_collect().await?;

        Ok(objects
            .into_iter()
            .map(|object| BucketFile { file_path: object.location.to_string(), file_size: object.size })
            .collect())
    }

    /// Returns `true` if a given file path exists.
    pub async fn exists(&self, file_path: &str) -> Result<bool> {
        match self.store.head(&Path::from(file_path)).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    /// Returns a copy of an object at the given file path if it exists.
    pub async fn get(&self, file_path: &str) -> Result<Vec<u8>> {
        Ok(self.store.get(&Path::from(file_path)).await?.bytes().await?.to_vec())
    }

    /// Inserts a new file at the given file path, if it does not exist.
    pub async fn insert(&self, file_path: &str, file: &[u8]) -> Result<()> {
        if self.exists(file_path).await? {
            bail!("File already exists: {file_path}")
        }

        self.insert_or_update(file_path, file).await
    }

    /// Updates a file at the given file path, if it does not exist, create one.
    pub async fn insert_or_update(&self, file_path: &str, file: &[u8]) -> Result<()> {
        let mut attributes = Attributes::new();
        attributes.insert(Attribute::CacheControl, CACHE_CONTROL.into());
        let options = PutOptions { attributes, ..Default::default() };

        self.store.put_opts(&Path::from(file_path), PutPayload::from(file.to_vec()), options).await?;

        Ok(())
    }

    /// Removes the object corresponding to the given file path.
    pub async fn remove(&self, file_path: &str) -> Result<()> {
        if !self.exists(file_path).await? {
            bail!("File does not exist: {file_path}")
        }

        self.store.delete(&Path::from(file_path)).await?;

        Ok(())
    }
}
