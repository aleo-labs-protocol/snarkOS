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

pub mod client;
pub mod permissions;
pub mod retry;

use crate::bucket::{client::Client, permissions::Permission};

use anyhow::{Result, bail};
use core::ops::Deref;
use rusoto_core::{RusotoError, request::HttpDispatchError};
use rusoto_s3::{GetObjectError, ListObjectsOutput, ListObjectsV2Output};
use tokio::io::{AsyncReadExt, BufReader};

#[derive(Debug, Clone)]
pub struct BucketFile {
    pub file_path: String,
    pub file_size: i64,
    pub last_modified: String,
}

#[derive(Clone)]
pub struct Bucket {
    client: Client,
    bucket_name: String,
}

impl Bucket {
    /// Initializes a new instance of the bucket.
    pub fn new(region: &str, endpoint: &str, bucket_name: &str, key: String, secret: String) -> Self {
        Self {
            client: Client::new(region.to_string(), endpoint.to_string(), key, secret, None),
            bucket_name: bucket_name.to_string(),
        }
    }

    /// Returns the bucket name.
    pub fn name(&self) -> &str {
        &self.bucket_name
    }

    /// Returns a list of all files in the bucket.
    pub async fn list_files(&self) -> Result<Vec<BucketFile>> {
        // Initialize the list of files.
        let mut files = Vec::new();

        // Initialize the marker.
        let mut marker = None;

        // Loop until the marker is None.
        loop {
            // Get the list of objects.
            let list_objects_output = self.client.list_files(self.bucket_name.clone(), None, marker).await?;

            // Get the objects.
            if let ListObjectsOutput { contents: Some(contents), .. } = &list_objects_output {
                // Convert the objects to files.
                for object in contents {
                    // Construct the file.
                    let file = match object {
                        rusoto_s3::Object {
                            key: Some(key),
                            size: Some(file_size),
                            last_modified: Some(last_modified),
                            ..
                        } => BucketFile {
                            file_path: key.clone(),
                            file_size: *file_size,
                            last_modified: last_modified.clone(),
                        },
                        _ => bail!("Unexpected object format in bucket"),
                    };

                    // Add the file to the list.
                    files.push(file);
                }
            }

            // Update the marker.
            marker = if list_objects_output.delimiter.is_none() && list_objects_output.is_truncated.unwrap_or(false) {
                files.last().map(|file| file.file_path.clone())
            } else {
                list_objects_output.next_marker.clone()
            };
            trace!("marker={:?} len={}", marker, files.len());
            // trace!("marker={:?} len={}\n{:#?}", marker, files.len(), list_objects_output);

            // Check if the marker is None.
            if marker.is_none() {
                // Break the loop.
                break;
            }
        }

        // Return the list of files.
        Ok(files)
    }

    /// Returns `true` if a given file path exists. Otherwise, returns `false`.
    pub async fn exists(&self, file_path: &str) -> bool {
        let object = self.client.get_file(self.bucket_name.clone(), file_path.to_string()).await;

        object.is_ok()
    }

    /// Returns a copy of an object at the given file path if it exists.
    pub async fn get(&self, file_path: &str) -> Result<Vec<u8>, RusotoError<GetObjectError>> {
        let object = self.client.get_file(self.bucket_name.clone(), file_path.to_string()).await?;

        // Get the file size.
        let size = match object.content_length {
            Some(size) => size,
            None => return Ok(vec![]),
        };

        match object.body {
            Some(body) => {
                let mut reader = BufReader::new(body.into_async_read());

                let mut buffer = Vec::with_capacity(size as usize);
                reader.read_to_end(&mut buffer).await?;

                Ok(buffer)
            }
            None => {
                let message = format!("Failed to get file: {file_path}");
                Err(RusotoError::HttpDispatch(HttpDispatchError::new(message)))
            }
        }
    }

    /// Inserts a new file at the given file path, if it does not exist.
    pub async fn insert(&self, file_path: &str, file: &[u8], is_public: bool) -> Result<()> {
        // Check if the file path already exists.
        if self.exists(file_path).await {
            bail!("File already exists: {file_path}")
        }

        let permission = match is_public {
            true => Permission::PublicRead,
            false => Permission::Private,
        };

        self.client
            .create_file(self.bucket_name.clone(), file_path.to_string(), permission, Some(file.to_vec()))
            .await?;

        Ok(())
    }

    /// Updates a file at the given file path, if it does not exist, create one.
    pub async fn insert_or_update(&self, file_path: &str, file: &[u8], is_public: bool) -> Result<()> {
        let permission = match is_public {
            true => Permission::PublicRead,
            false => Permission::Private,
        };

        self.client
            .create_file(self.bucket_name.clone(), file_path.to_string(), permission, Some(file.to_vec()))
            .await?;

        Ok(())
    }

    /// Removes the object corresponding to the given file path.
    pub async fn remove(&self, file_path: &str) -> Result<()> {
        // Check if the file exists.
        if !self.exists(file_path).await {
            bail!("File does not exist: {file_path}")
        }

        // Delete the file.
        self.client.delete_file(self.bucket_name.clone(), file_path.to_string()).await?;

        Ok(())
    }

    /// Returns the size in bytes of the object stored at the given file path.
    pub async fn size(&self, file_path: &str) -> Result<i64> {
        // Get the object.
        let object = self.client.get_file(self.bucket_name.clone(), file_path.to_string()).await;

        // Get the file.
        let file = match object {
            Ok(file) => file,
            Err(_) => bail!("File does not exist: {file_path}"),
        };

        // Get the file size.
        match file.content_length {
            Some(size) => Ok(size),
            None => bail!("File body is empty: {file_path}"),
        }
    }

    /// Returns a list of objects in the given bucket starting from the given key.
    pub async fn list_objects_after_key(
        &self,
        key: String,
        continuation_token: Option<String>,
    ) -> Result<ListObjectsV2Output> {
        self.client.list_objects_after_key(self.bucket_name.clone(), key, continuation_token).await
    }
}

impl Deref for Bucket {
    type Target = Client;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}
