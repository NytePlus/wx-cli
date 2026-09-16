mod index;
mod media;
use anyhow::{bail, Context, Result};
use index::{text, Index};
use rusqlite::{params, OptionalExtension};
use serde_json::{json, Value};
use std::{
    io::{Read, Write},
    path::PathBuf,
    sync::mpsc,
    time::Duration,
};
#[cfg(test)]
use wx_db::incremental::MessagePosition;
use wx_db::{ChatRoomQuery, ContactQuery, WechatDb};

const MAX_FRAME: usize = 4 * 1024 * 1024;
fn read_frame(r: &mut impl Read) -> Result<Option<Value>> {
    let mut n = [0; 4];
    match r.read(&mut n[..1])? {
        0 => return Ok(None),
        _ => r.read_exact(&mut n[1..])?,
    };
    let len = u32::from_be_bytes(n) as usize;
    if len == 0 || len > MAX_FRAME {
        bail!("invalid_frame_length");
    }
    let mut data = vec![0; len];
    r.read_exact(&mut data)?;
    Ok(Some(serde_json::from_slice(&data)?))
}
fn write_frame(w: &mut impl Write, v: &Value) -> Result<()> {
    let data = serde_json::to_vec(v)?;
    if data.len() > MAX_FRAME {
        bail!("response_too_large");
    }
    w.write_all(&(data.len() as u32).to_be_bytes())?;
    w.write_all(&data)?;
    w.flush()?;
    Ok(())
}

struct Backend {
    index: Option<Index>,
    source: Option<WechatDb>,
    error: Option<String>,
    root: PathBuf,
    archive_path: PathBuf,
    has_import_work: bool,
    image_key: Option<[u8; 16]>,
    image_config_unavailable: bool,
    session_diagnostics: Vec<String>,
}
impl Backend {
    fn new() -> Self {
        Self {
            index: None,
            source: None,
            error: None,
            root: PathBuf::new(),
            archive_path: PathBuf::new(),
            has_import_work: false,
            image_key: None,
            image_config_unavailable: false,
            session_diagnostics: Vec::new(),
        }
    }
    fn request(&mut self, method: &str, p: &Value) -> Result<Value> {
        if method == "initialize" {
            let root = PathBuf::from(text(p, "data_root"));
            let key = hex::decode(text(p, "key")).context("invalid_key")?;
            let raw: [u8; 32] = key
                .try_into()
                .map_err(|_| anyhow::anyhow!("invalid_key_length"))?;
            let account = text(p, "account");
            if account.is_empty() {
                bail!("account_required");
            }
            let uin = text(p, "image_uin");
            self.image_config_unavailable =
                p["image_config_unavailable"].as_bool().unwrap_or(false);
            self.image_key = if uin.is_empty() {
                None
            } else {
                if uin.len() > 32 || !uin.bytes().all(|c| c.is_ascii_digit()) {
                    bail!("invalid_image_config");
                }
                Some(wx_media::derive_v2_aes_key(
                    &uin,
                    &wx_media::extract_wxid(&account),
                ))
            };
            self.archive_path = PathBuf::from(text(p, "index_path"));
            let media_dir = self.archive_path.with_extension("media");
            if media_dir.is_dir() {
                for entry in std::fs::read_dir(&media_dir)? {
                    let e = entry?;
                    if e.file_type()?.is_file()
                        && e.file_name().to_string_lossy().starts_with("asset-")
                    {
                        std::fs::remove_file(e.path())?;
                    }
                }
            }
            self.index = Some(Index::open(&self.archive_path, &account)?);
            // One-time cursor migration. Preserve first-seen archived bodies,
            // but restart incomplete initial imports with a rowid cursor.
            let db = &self.index.as_ref().unwrap().db;
            let migrated: bool = db.query_row(
                "SELECT EXISTS(SELECT 1 FROM settings WHERE key='history_rowid_v1')",
                [],
                |r| r.get(0),
            )?;
            if !migrated {
                let tx = db.unchecked_transaction()?;
                tx.execute("UPDATE import_positions SET position='-9223372036854775808',done=0 WHERE conversation IN (SELECT id FROM conversations WHERE state IN ('indexing','incompatible_message_index'))",[])?;
                tx.execute("UPDATE conversations SET state='indexing' WHERE state='incompatible_message_index'",[])?;
                tx.execute(
                    "INSERT INTO settings(key,value) VALUES('history_rowid_v1','1')",
                    [],
                )?;
                tx.commit()?;
            }
            // Retry incomplete imports after the TEXT/BLOB compatibility fix.
            db.execute("UPDATE conversations SET state='indexing' WHERE state LIKE 'history_column_type_error%'",[])?;
            // One startup recovery probe; no archive queries while idle thereafter.
            self.has_import_work = true;
            self.root = if root.join("db_storage").is_dir() {
                root.join("db_storage")
            } else {
                root
            };
            match WechatDb::open_encrypted(&self.root, raw) {
                Ok(db) => {
                    self.session_diagnostics = db
                        .session_index_diagnostics()
                        .unwrap_or_else(|_| vec!["schema_diagnostic_failed".into()]);
                    self.error=Some(match db.session_changes_page(0,"",1) {
                        Ok(_)=>"live_sync_unverified: session timestamp and shard routing are not a durable change log".to_owned(),
                        Err(_)=>"incompatible_session_range_index".to_owned()
                    });
                    self.source = Some(db);
                }
                Err(_) => {
                    self.source = None;
                    self.has_import_work = false;
                    self.error = Some("source_unavailable_or_invalid_key".into());
                }
            }
            return self.request("status", &json!({}));
        }
        let idx = self.index.as_ref().context("setup_required")?;
        match method {
            "status" => {
                let bytes = std::fs::metadata(&self.archive_path)
                    .map(|m| m.len())
                    .unwrap_or(0)
                    + std::fs::metadata(format!("{}-wal", self.archive_path.display()))
                        .map(|m| m.len())
                        .unwrap_or(0);
                let quota: i64 = idx.db.query_row(
                    "SELECT CAST(value AS INTEGER) FROM settings WHERE key='quota'",
                    [],
                    |r| r.get(0),
                )?;
                Ok(
                    json!({"source_available":self.source.is_some(),"session_index_diagnostics":self.session_diagnostics,"image_key_configured":self.image_key.is_some(),"image_config_unavailable":self.image_config_unavailable,"live_sync_ready":false,"compatibility":self.error,"index_bytes":bytes,"quota_bytes":quota,"quota_warning":bytes>quota as u64,"conversations":idx.list("",false)?["items"],"archive_semantics":"first_observed"}),
                )
            }
            "find_conversations" => idx.list(&text(p, "query"), false),
            "local_list" => idx.list("", true),
            "local_discover" => {
                let source = self.source.as_ref().context("source_unavailable")?;
                let result = source
                    .query_contacts(&ContactQuery::new().keyword(text(p, "query")).limit(200))?;
                Ok(
                    json!({"items":result.items.iter().map(|c|json!({"conversation_id":c.user_name,"name":if !c.remark.is_empty(){&c.remark}else{&c.nick_name}})).collect::<Vec<_>>()}),
                )
            }
            "local_grant" => {
                let source = self.source.as_ref().context("source_unavailable")?;
                let id = text(p, "conversation_id");
                if id.is_empty() {
                    bail!("conversation_required");
                }
                idx.grant(&id, &text(p, "name"))?;
                self.has_import_work = true;
                for shard in source.source_shards() {
                    idx.db.execute(
                        "INSERT OR IGNORE INTO import_positions VALUES(?1,?2,?3,0)",
                        params![id, shard, i64::MIN.to_string()],
                    )?;
                }
                if let Ok(rooms) = source.query_chatrooms(&ChatRoomQuery::new().username(&id)) {
                    for room in rooms.items {
                        for member in room.members {
                            idx.db.execute(
                                "INSERT OR IGNORE INTO members VALUES(?1,?2,?3)",
                                params![
                                    id,
                                    member.user_name,
                                    member
                                        .display_name
                                        .unwrap_or_else(|| member.user_name.clone())
                                ],
                            )?;
                        }
                    }
                }
                Ok(json!({"approved":true,"conversation_id":id,"state":"indexing"}))
            }
            "local_revoke" => {
                idx.revoke(
                    &text(p, "conversation_id"),
                    p["delete"].as_bool().unwrap_or(false),
                )?;
                Ok(json!({"ok":true}))
            }
            "local_quota" => {
                let n = p["bytes"]
                    .as_i64()
                    .filter(|n| *n >= 1024 * 1024)
                    .context("invalid_quota")?;
                idx.db.execute(
                    "UPDATE settings SET value=?1 WHERE key='quota'",
                    [n.to_string()],
                )?;
                Ok(json!({"ok":true}))
            }
            "get_messages" | "search_messages" => idx.query(p, false),
            "get_updates" => idx.query(p, true),
            "list_members" => idx.members(p),
            "get_message_context" => {
                let id = text(p, "message_id");
                let row:Option<(String,i64)>=idx.db.query_row("SELECT m.conversation,m.time FROM messages m JOIN conversations c ON c.id=m.conversation AND c.enabled=1 WHERE m.id=?1",[&id],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
                let (conv, time) = row.context("message_unavailable")?;
                let n = p["context"].as_i64().unwrap_or(20).clamp(1, 100);
                let mut stmt=idx.db.prepare("SELECT data FROM (SELECT * FROM messages WHERE conversation=?1 AND (time,id)<(?2,?3) ORDER BY time DESC,id DESC LIMIT ?4) UNION ALL SELECT data FROM (SELECT * FROM messages WHERE conversation=?1 AND (time,id)>=(?2,?3) ORDER BY time,id LIMIT ?4)")?;
                let mut items = stmt
                    .query_map(params![conv, time, id, n], |r| r.get::<_, String>(0))?
                    .map(|r| Ok(serde_json::from_str::<Value>(&r?)?))
                    .collect::<Result<Vec<_>>>()?;
                items.sort_by_key(|m| {
                    (
                        m["create_time"].as_i64().unwrap_or(0),
                        text(m, "message_id"),
                    )
                });
                Ok(json!({"items":items}))
            }
            "authorize_message" | "get_media" => {
                let row:Option<String>=idx.db.query_row("SELECT m.data FROM messages m JOIN conversations c ON c.id=m.conversation AND c.enabled=1 WHERE m.id=?1",[text(p,"message_id")],|r|r.get(0)).optional()?;
                let data: Value = serde_json::from_str(&row.context("message_unavailable")?)?;
                if method == "authorize_message" {
                    Ok(json!({"authorized":true}))
                } else {
                    media::extract(
                        self.source.as_ref().context("source_unavailable")?,
                        &self.root,
                        &self.archive_path.with_extension("media"),
                        &data,
                        self.image_key,
                    )
                }
            }
            _ => bail!("unknown_method"),
        }
    }
    fn import_batch(&mut self) -> Result<()> {
        if !self.has_import_work {
            return Ok(());
        }
        let (Some(idx), Some(source)) = (&self.index, &self.source) else {
            return Ok(());
        };
        let pending:Option<(String,String,String)>=idx.db.query_row("SELECT p.conversation,p.shard,p.position FROM import_positions p JOIN conversations c ON c.id=p.conversation WHERE p.done=0 AND c.enabled=1 AND c.state='indexing' LIMIT 1",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
        let Some((conv, shard, pos)) = pending else {
            self.has_import_work = false;
            return Ok(());
        };
        let result = source.history_page(&shard, &conv, pos.parse()?, 128);
        let (batch, last) = match result {
            Ok(b) => b,
            Err(e) => {
                // Error categories only: never include database content or keys.
                let category = match e {
                    wx_db::DbError::Sqlite(rusqlite::Error::InvalidColumnType(column, _, kind)) => {
                        let state = format!("history_column_type_error:{column}:{kind:?}");
                        idx.db.execute(
                            "UPDATE conversations SET state=?2 WHERE id=?1",
                            params![conv, state],
                        )?;
                        return Ok(());
                    }
                    wx_db::DbError::Sqlite(_) => "history_sql_error",
                    wx_db::DbError::FtsInit(_) => "history_range_index_missing",
                    wx_db::DbError::Zstd(_) => "history_decode_error",
                    wx_db::DbError::EncryptionKey(_) => "history_key_error",
                    _ => "history_read_failed",
                };
                idx.db.execute(
                    "UPDATE conversations SET state=?2 WHERE id=?1",
                    params![conv, category],
                )?;
                return Ok(());
            }
        };
        let tx = idx.db.unchecked_transaction()?;
        for m in &batch {
            idx.insert(m)?;
        }
        tx.execute(
            "UPDATE import_positions SET position=?3,done=?4 WHERE conversation=?1 AND shard=?2",
            params![conv, shard, last.to_string(), batch.len() < 128],
        )?;
        tx.execute("UPDATE conversations SET state='ready' WHERE id=?1 AND NOT EXISTS(SELECT 1 FROM import_positions WHERE conversation=?1 AND done=0)",[conv])?;
        tx.commit()?;
        Ok(())
    }
}
fn run() -> Result<()> {
    #[cfg(unix)]
    unsafe {
        libc_umask();
    }
    let (tx, rx) = mpsc::sync_channel(16);
    std::thread::spawn(move || {
        let mut input = std::io::stdin().lock();
        while let Ok(Some(v)) = read_frame(&mut input) {
            if tx.send(v).is_err() {
                break;
            }
        }
    });
    let mut backend = Backend::new();
    let mut output = std::io::stdout().lock();
    loop {
        // Block on IPC while idle. Only initial-import work needs timer ticks.
        let event = if backend.has_import_work {
            rx.recv_timeout(Duration::from_millis(100))
        } else {
            rx.recv().map_err(|_| mpsc::RecvTimeoutError::Disconnected)
        };
        match event {
            Ok(v) => {
                let id = v["id"].clone();
                let response = match backend.request(&text(&v, "method"), &v["params"]) {
                    Ok(result) => json!({"jsonrpc":"2.0","id":id,"result":result}),
                    Err(e) => {
                        json!({"jsonrpc":"2.0","id":id,"error":{"code":-32000,"message":e.to_string()}})
                    }
                };
                write_frame(&mut output, &response)?;
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
            Err(mpsc::RecvTimeoutError::Timeout) => {}
        }
        if backend.import_batch().is_err() {
            backend.has_import_work = false;
            backend.error = Some("index_write_failed".into());
        }
    }
    Ok(())
}
#[cfg(unix)]
unsafe fn libc_umask() {
    unsafe extern "C" {
        fn umask(mask: u16) -> u16;
    }
    let _ = umask(0o077);
}
fn main() {
    if run().is_err() {
        eprintln!("wechat backend stopped: protocol or storage error");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wx_db::{incremental::SourceMessage, Message, MessageContent};
    fn fixture() -> (tempfile::TempDir, Index) {
        let d = tempfile::tempdir().unwrap();
        let i = Index::open(&d.path().join("index.db"), "a").unwrap();
        i.grant("g", "群").unwrap();
        i.db.execute("UPDATE conversations SET state='ready'", [])
            .unwrap();
        (d, i)
    }
    fn add(i: &Index, id: i64, member: &str, c: MessageContent) {
        i.insert(&SourceMessage {
            position: MessagePosition {
                local_id: id,
                ..Default::default()
            },
            shard_id: "message_0.db".into(),
            message: Message {
                sort_seq: id,
                server_id: id,
                msg_type: 1,
                sub_type: 0,
                sender: member.into(),
                talker: "g".into(),
                create_time: 1700000000 + id,
                content: c,
                status: 0,
            },
        })
        .unwrap();
    }
    #[test]
    fn combined_filters_and_revocation() {
        let (_d, i) = fixture();
        add(&i, 1, "a", MessageContent::Text("博士开会".into()));
        add(&i, 2, "b", MessageContent::Text("博士开会".into()));
        add(&i, 3, "a", MessageContent::Image { md5: None });
        let p = json!({"conversation":"群","members":["a"],"types":["text","file"],"keyword":"博士","start":"2023-11-14T22:13:20Z","end":"2023-11-14T22:13:23Z"});
        assert_eq!(
            i.query(&p, false).unwrap()["items"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        i.revoke("g", false).unwrap();
        assert!(i.query(&p, false).is_err());
        assert!(i.list("", false).unwrap()["items"]
            .as_array()
            .unwrap()
            .is_empty());
    }
    #[test]
    fn stable_pages_and_scope_bound_cursor() {
        let (_d, i) = fixture();
        for id in 1..5 {
            add(&i, id, "a", MessageContent::Text("x".into()));
        }
        let mut p = json!({"conversation":"g","limit":2});
        let r = i.query(&p, false).unwrap();
        add(&i, 5, "a", MessageContent::Text("later".into()));
        p["cursor"] = r["next_cursor"].clone();
        let r2 = i.query(&p, false).unwrap();
        assert_eq!(r2["items"][0]["message_id"], "g:2");
        p["types"] = json!(["image"]);
        assert!(i.query(&p, false).is_err());
    }
    #[test]
    fn updates_retry_does_not_consume() {
        let (_d, i) = fixture();
        add(&i, 1, "a", MessageContent::Text("x".into()));
        let p = json!({"conversation":"g"});
        assert_eq!(i.query(&p, true).unwrap(), i.query(&p, true).unwrap());
        let r = i.query(&p, true).unwrap();
        let next = json!({"conversation":"g","cursor":r["next_cursor"]});
        assert!(i.query(&next, true).unwrap()["items"]
            .as_array()
            .unwrap()
            .is_empty());
    }
    #[test]
    fn forged_and_cross_account_cursors_rejected() {
        let (_d, i) = fixture();
        let mut p = json!({"conversation":"g"});
        add(&i, 1, "a", MessageContent::Text("x".into()));
        let r = i.query(&p, false).unwrap();
        let mut c = text(&r, "next_cursor");
        c.push('0');
        p["cursor"] = json!(c);
        assert!(i.query(&p, false).is_err());
    }
    #[test]
    fn protocol_truncation_and_size() {
        assert!(read_frame(&mut &b"\0\0\0\x05{}"[..]).is_err());
        assert!(read_frame(&mut &u32::MAX.to_be_bytes()[..]).is_err());
        let mut b = vec![];
        write_frame(&mut b, &json!({"id":1})).unwrap();
        assert_eq!(read_frame(&mut &b[..]).unwrap().unwrap()["id"], 1);
    }
    #[test]
    fn idle_import_does_not_read_archive() {
        let (_d, i) = fixture();
        // Removing the scheduler table makes an accidental idle query fail.
        i.db.execute("DROP TABLE import_positions", []).unwrap();
        let mut backend = Backend::new();
        backend.index = Some(i);
        for _ in 0..10_000 {
            backend.import_batch().unwrap();
        }
        assert!(!backend.has_import_work);
    }
    #[test]
    fn cursor_rejects_other_account_and_revoked_generation() {
        let (_d, i) = fixture();
        add(&i, 1, "a", MessageContent::Text("hello".into()));
        let r = i.query(&json!({"conversation":"g"}), true).unwrap();
        let p = json!({"conversation":"g", "cursor":r["next_cursor"]});
        let d2 = tempfile::tempdir().unwrap();
        let other = Index::open(&d2.path().join("index.db"), "other").unwrap();
        other.grant("g", "群").unwrap();
        other
            .db
            .execute("UPDATE conversations SET state='ready'", [])
            .unwrap();
        assert!(other.query(&p, true).is_err());
        i.revoke("g", false).unwrap();
        i.grant("g", "群").unwrap();
        assert!(i.query(&p, true).is_err());
    }
}
