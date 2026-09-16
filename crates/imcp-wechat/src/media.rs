use anyhow::{bail, Context, Result};
use serde_json::{json, Value};
use std::{
    io::{Read, Write},
    path::{Path, PathBuf},
};
use wx_db::WechatDb;
const MAX: u64 = 100 * 1024 * 1024;
fn safe_read(root: &Path, path: &Path) -> Result<Vec<u8>> {
    let actual = path.canonicalize()?;
    if !actual.starts_with(root.canonicalize()?) {
        bail!("media_path_outside_account");
    }
    let mut bytes = Vec::new();
    std::fs::File::open(actual)?
        .take(MAX + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX {
        bail!("media_exceeds_100_MB");
    }
    Ok(bytes)
}
pub fn extract(
    db: &WechatDb,
    root: &Path,
    temp: &Path,
    msg: &Value,
    image_key: Option<[u8; 16]>,
) -> Result<Value> {
    let account = root.parent().context("invalid_account_root")?;
    let kind = msg["type"].as_str().unwrap_or("");
    let content = &msg["content"];
    let (bytes, mime, ext) = match kind {
        "image" => {
            let md5 = content["Image"]["md5"]
                .as_str()
                .context("image_reference_missing")?;
            let attach = account.join("msg/attach");
            let path = wx_media::resolve_image_by_md5(
                msg["conversation_id"]
                    .as_str()
                    .context("conversation_required")?,
                &attach,
                md5,
            )?
            .recommended
            .context("image_not_available_locally")?;
            let input = safe_read(account, &path)?;
            // Only use explicit config-derived key; never probe shared directories
            // outside the account's security-scoped bookmark.
            let opts = wx_media::DatDecryptOptions {
                v2_aes_key: image_key,
                xor_key: wx_media::detect_xor_key(path.parent().unwrap_or(&attach)),
            };
            let mut decoded = wx_media::decrypt_dat(&input, &opts)?;
            if decoded.ext == "wxgf" {
                let converted = wx_media::transcode_wxgf(&decoded.data)?;
                if !converted.transcoded {
                    bail!("wxgf_transcoder_unavailable");
                }
                decoded.data = converted.data;
                decoded.ext = converted.ext.to_owned();
            }
            let mime = match decoded.ext.as_str() {
                "jpg" | "jpeg" => "image/jpeg",
                "png" => "image/png",
                "gif" => "image/gif",
                "webp" => "image/webp",
                _ => "application/octet-stream",
            };
            (decoded.data, mime, decoded.ext.to_owned())
        }
        "voice" => {
            let server = msg["source"]["server_id"]
                .as_i64()
                .context("voice_reference_missing")?
                .to_string();
            let mut found = None;
            for path in wx_media::find_media_dbs(&root.join("media"))? {
                let c = db.open_related_readonly(&path)?;
                if let Ok(blob) = wx_media::extract_voice_with_conn(&c, &server) {
                    found = Some(blob.data);
                    break;
                }
            }
            (
                found.context("voice_not_available_locally")?,
                "audio/silk",
                "silk".into(),
            )
        }
        "file" | "video" => {
            let variant = if kind == "file" { "File" } else { "Video" };
            let md5 = content[variant]["md5"]
                .as_str()
                .context("media_reference_missing")?;
            let c = db.open_related_readonly(&root.join("hardlink/hardlink.db"))?;
            let entries = wx_media::query_hardlink_with_conn(&c, kind, md5)?;
            let base = account.join("msg").join(kind);
            let mut candidate: Option<PathBuf> = None;
            for e in entries {
                for path in [
                    base.join(&e.dir1).join(&e.dir2).join(&e.file_name),
                    base.join(&e.dir1).join(&e.file_name),
                ] {
                    if path.is_file() {
                        candidate = Some(path);
                        break;
                    }
                }
                if candidate.is_some() {
                    break;
                }
            }
            let path = candidate.context("media_not_available_locally")?;
            (
                safe_read(account, &path)?,
                if kind == "video" {
                    "video/mp4"
                } else {
                    "application/octet-stream"
                },
                "bin".into(),
            )
        }
        _ => bail!("message_has_no_supported_media"),
    };
    if bytes.len() as u64 > MAX {
        bail!("media_exceeds_100_MB");
    }
    std::fs::create_dir_all(temp)?;
    let mut f = tempfile::Builder::new()
        .prefix("asset-")
        .suffix(&format!(".{ext}"))
        .tempfile_in(temp)?;
    f.write_all(&bytes)?;
    let (_, path) = f.keep()?;
    Ok(json!({"path":path,"mime_type":mime,"size":bytes.len(),"message_id":msg["message_id"]}))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_outside_path() {
        let a = tempfile::tempdir().unwrap();
        let b = tempfile::tempdir().unwrap();
        std::fs::write(b.path().join("secret"), "x").unwrap();
        assert!(safe_read(a.path(), &b.path().join("secret")).is_err());
    }
}
