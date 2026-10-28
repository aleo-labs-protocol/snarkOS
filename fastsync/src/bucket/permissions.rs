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

use crate::client::Client;

use anyhow::Result;
use rusoto_s3::{
    GetBucketAclOutput,
    GetBucketAclRequest,
    GetObjectAclOutput,
    GetObjectAclRequest,
    PutBucketAclRequest,
    PutObjectAclOutput,
    PutObjectAclRequest,
    S3,
};
use std::fmt;

/// Bucket or object ACL (Access Control List)
#[derive(Copy, Clone)]
pub enum Permission {
    Private,
    PublicRead,
}

impl fmt::Display for Permission {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Permission::Private => write!(f, "private"),
            Permission::PublicRead => write!(f, "public-read"),
        }
    }
}

/// Manage permissions from the client
impl Client {
    pub async fn update_bucket_acl(&self, bucket: String, acl: Permission) -> Result<()> {
        let put_bucket_acl_request = PutBucketAclRequest { bucket, acl: Some(acl.to_string()), ..Default::default() };

        Ok(self.s3().put_bucket_acl(put_bucket_acl_request).await?)
    }

    pub async fn get_bucket_acl(&self, bucket: String) -> Result<GetBucketAclOutput> {
        let get_bucket_acl_request = GetBucketAclRequest { bucket, expected_bucket_owner: None };

        Ok(self.s3().get_bucket_acl(get_bucket_acl_request).await?)
    }

    pub async fn update_file_acl(
        &self,
        bucket: String,
        file_name: String,
        acl: Permission,
    ) -> Result<PutObjectAclOutput> {
        let put_object_acl_request =
            PutObjectAclRequest { bucket, key: file_name, acl: Some(acl.to_string()), ..Default::default() };

        Ok(self.s3().put_object_acl(put_object_acl_request).await?)
    }

    pub async fn get_file_acl(&self, bucket: String, file_name: String) -> Result<GetObjectAclOutput> {
        let get_object_acl_request = GetObjectAclRequest { bucket, key: file_name, ..Default::default() };

        Ok(self.s3().get_object_acl(get_object_acl_request).await?)
    }
}
