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

use snarkvm::prelude::*;

use ::time::OffsetDateTime;
use anyhow::{Result, anyhow};
use axum::{
    RequestPartsExt,
    body::Body,
    http::{Request, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use axum_extra::{
    TypedHeader,
    headers::authorization::{Authorization, Bearer},
};
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, encode};
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};

/// The time a jwt token is valid for.
pub const EXPIRATION: i64 = 10 * 365 * 24 * 60 * 60; // 10 years.

/// The JWT secret for the REST server.
static JWT_SECRET: OnceCell<Vec<u8>> = OnceCell::new();

/// The Json web token claims.
#[derive(Debug, Deserialize, Serialize)]
pub struct Claims {
    /// The subject (user).
    sub: String,
    /// The UTC timestamp the token was issued at.
    iat: i64,
    /// Expiration time (as UTC timestamp).
    exp: i64,
}

impl Claims {
    pub fn new<N: Network>(address: Address<N>, jwt_secret: Option<Vec<u8>>, jwt_timestamp: Option<i64>) -> Self {
        if let Some(secret) = jwt_secret {
            JWT_SECRET.set(secret)
        } else {
            JWT_SECRET.set({
                let seed: [u8; 16] = ::rand::rng().random();
                seed.to_vec()
            })
        }
        .expect("Failed to set JWT secret: already initialized");

        let issued_at = jwt_timestamp.unwrap_or_else(|| OffsetDateTime::now_utc().unix_timestamp());
        let expiration = issued_at.saturating_add(EXPIRATION);

        Self { sub: address.to_string(), iat: issued_at, exp: expiration }
    }

    /// Returns the json web token string.
    pub fn to_jwt_string(&self) -> Result<String> {
        let key = EncodingKey::from_secret(JWT_SECRET.get().unwrap()).map_err(|e| anyhow!(e))?;
        encode(&Header::default(), &self, &key).map_err(|e| anyhow!(e))
    }
}

pub async fn auth_middleware(request: Request<Body>, next: Next) -> Result<Response, Response> {
    // If the JWT secret is not set, skip authentication.
    if JWT_SECRET.get().is_none() {
        return Ok(next.run(request).await);
    }

    // Deconstruct the request to extract the auth token.
    let (mut parts, body) = request.into_parts();
    let auth: TypedHeader<Authorization<Bearer>> =
        parts.extract().await.map_err(|_| StatusCode::UNAUTHORIZED.into_response())?;

    if let Err(err) = DecodingKey::from_secret(JWT_SECRET.get().unwrap())
        .and_then(|key| decode::<Claims>(auth.token(), &key, &Validation::new(Algorithm::HS256)))
    {
        warn!("Request authorization error: {err}");
        return Err(StatusCode::UNAUTHORIZED.into_response());
    }

    // Reconstruct the request.
    let request = Request::from_parts(parts, body);

    Ok(next.run(request).await)
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::prelude::*;
    use snarkvm::prelude::{Address, MainnetV0};

    #[test]
    fn check_const_jwt_value() {
        // Arbitrary input values to check against the expected value.
        let secret = "FVPjEPVAKh2f0EkRCpQkqA==";
        let timestamp = 174437065;

        let secret_bytes = BASE64_STANDARD.decode(secret).unwrap();

        // A fixed seed, as the address also forms part of the JWT.
        let mut rng = TestRng::fixed(12345);
        let pk = PrivateKey::<MainnetV0>::new(&mut rng).unwrap();
        let addr = Address::try_from(pk).unwrap();

        let claims = Claims::new(addr, Some(secret_bytes), Some(timestamp));
        let jwt_str = claims.to_jwt_string().unwrap();

        assert_eq!(
            jwt_str,
            "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.\
            eyJzdWIiOiJhbGVvMTBrbmtlbHZuZDU1ZnNhYX\
            JtMjV3Y2g3cDlzdWYydHFsZ3d5NWs0bnh3bXM2\
            ZDI2Mnh5ZnFtMnRjY3IiLCJpYXQiOjE3NDQzNz\
            A2NSwiZXhwIjo0ODk3OTcwNjV9.HcTvPC7jQyq\
            NaPqsC2XHZl3Yji_OHxo5TyKLSKVxirI"
        );
    }
}
