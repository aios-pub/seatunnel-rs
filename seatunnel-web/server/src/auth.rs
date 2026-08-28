/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */

//! Username/password authentication with HMAC-signed session cookies.
//!
//! Single-account model: credentials come from CLI flags / environment
//! (`--auth-user`, `--auth-password`, `SEATUNNEL_WEB_PASSWORD`). Session
//! tokens are self-contained `<expiry_ms>.<hmac>` values signed with a
//! random per-boot key, so sessions survive handler changes but all
//! sessions are invalidated when the process restarts.

use hmac::{Hmac, Mac};
use sha2::Sha256;

/// Cookie name carrying the session token.
pub const SESSION_COOKIE: &str = "seatunnel_session";

type HmacSha256 = Hmac<Sha256>;

/// Console authentication settings.
#[derive(Clone)]
pub struct AuthConfig {
    username: String,
    password: String,
    /// Random signing key; regenerated on every boot.
    signing_key: [u8; 32],
    /// Session lifetime in seconds.
    ttl_secs: u64,
    /// `auth-disable` mode: every request is authorized.
    pub disabled: bool,
}

impl AuthConfig {
    pub fn new(username: String, password: String, ttl_secs: u64) -> Self {
        // Two random UUIDs give 32 bytes of key material without pulling in
        // a dedicated rng crate.
        let a = uuid::Uuid::new_v4();
        let b = uuid::Uuid::new_v4();
        let mut signing_key = [0u8; 32];
        signing_key[..16].copy_from_slice(a.as_bytes());
        signing_key[16..].copy_from_slice(b.as_bytes());
        AuthConfig {
            username,
            password,
            signing_key,
            ttl_secs,
            disabled: false,
        }
    }

    /// Authentication-free mode for local development.
    pub fn disabled() -> Self {
        AuthConfig {
            username: String::new(),
            password: String::new(),
            signing_key: [0u8; 32],
            ttl_secs: 0,
            disabled: true,
        }
    }

    /// The configured login name.
    pub fn username(&self) -> String {
        self.username.clone()
    }

    /// Constant-time byte comparison to avoid timing oracles.
    fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
        if a.len() != b.len() {
            return false;
        }
        let mut diff = 0u8;
        for (x, y) in a.iter().zip(b.iter()) {
            diff |= x ^ y;
        }
        diff == 0
    }

    /// Check the configured credentials. Bitwise `&` (not `&&`) on purpose:
    /// short-circuiting would leak whether the username matched.
    pub fn check_credentials(&self, username: &str, password: &str) -> bool {
        let user_ok = Self::constant_time_eq(username.as_bytes(), self.username.as_bytes());
        let pass_ok = Self::constant_time_eq(password.as_bytes(), self.password.as_bytes());
        user_ok & pass_ok
    }

    fn sign(&self, username: &str, expiry_ms: u64) -> String {
        let mut mac =
            HmacSha256::new_from_slice(&self.signing_key).expect("HMAC accepts any key length");
        mac.update(format!("{}:{}", username, expiry_ms).as_bytes());
        hex::encode(mac.finalize().into_bytes())
    }

    /// Issue a session token for the configured user.
    pub fn issue_token(&self, now_ms: u64) -> String {
        let expiry_ms = now_ms + self.ttl_secs * 1000;
        format!("{}:{}", expiry_ms, self.sign(&self.username, expiry_ms))
    }

    /// Verify a session token; returns the username on success.
    pub fn verify_token(&self, token: &str, now_ms: u64) -> Option<String> {
        let (expiry, signature) = token.split_once(':')?;
        let expiry: u64 = expiry.parse().ok()?;
        if expiry <= now_ms {
            return None;
        }
        let expected = self.sign(&self.username, expiry);
        // `constant_time_eq` instead of a direct comparison so signature
        // verification is not timing-leaky.
        if Self::constant_time_eq(signature.as_bytes(), expected.as_bytes()) {
            Some(self.username.clone())
        } else {
            None
        }
    }

    /// `Set-Cookie` header value for a freshly issued token.
    pub fn session_cookie(&self, token: &str) -> String {
        format!(
            "{}={}; Path=/; HttpOnly; SameSite=Strict; Max-Age={}",
            SESSION_COOKIE, token, self.ttl_secs
        )
    }

    /// `Set-Cookie` header value that clears the session.
    pub fn clearing_cookie() -> String {
        format!(
            "{}=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0",
            SESSION_COOKIE
        )
    }

    /// Extract the session token from a `Cookie` header value.
    pub fn token_from_cookie_header(header: &str) -> Option<&str> {
        header.split(';').find_map(|part| {
            let (name, value) = part.trim().split_once('=')?;
            (name == SESSION_COOKIE).then_some(value)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> AuthConfig {
        AuthConfig::new("admin".to_string(), "secret".to_string(), 60)
    }

    #[test]
    fn credentials_checked_constant_time() {
        let auth = config();
        assert!(auth.check_credentials("admin", "secret"));
        assert!(!auth.check_credentials("admin", "wrong"));
        assert!(!auth.check_credentials("root", "secret"));
        assert!(!auth.check_credentials("", ""));
    }

    #[test]
    fn token_roundtrip_and_expiry() {
        let auth = config();
        let token = auth.issue_token(1_000);
        assert_eq!(auth.verify_token(&token, 2_000).as_deref(), Some("admin"));
        // After expiry the token is rejected.
        assert_eq!(auth.verify_token(&token, 61_000), None);
        // Tampered signature is rejected.
        let tampered = format!("{}:{}", token.rsplit_once(':').unwrap().0, "00".repeat(32));
        assert_eq!(auth.verify_token(&tampered, 2_000), None);
        // A different boot (fresh key) invalidates old tokens.
        let other = config();
        assert_eq!(other.verify_token(&token, 2_000), None);
    }

    #[test]
    fn cookie_parsing() {
        assert_eq!(
            AuthConfig::token_from_cookie_header("a=b; seatunnel_session=tok; c=d"),
            Some("tok")
        );
        assert_eq!(AuthConfig::token_from_cookie_header("a=b"), None);
    }
}
