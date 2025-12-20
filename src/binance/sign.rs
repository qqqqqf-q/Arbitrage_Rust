use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

pub fn sign_query(secret: &str, query: &str) -> anyhow::Result<String> {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .map_err(|_| anyhow::anyhow!("HMAC 初始化失败"))?;
    mac.update(query.as_bytes());
    let signature = mac.finalize().into_bytes();
    Ok(hex::encode(signature))
}
