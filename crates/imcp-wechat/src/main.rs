mod index;
mod media;
use anyhow::{bail, Context, Result};
use index::{text, Index};
use rusqlite::{params, OptionalExtension};
use serde_json::{json, Value};
use std::{
    io::{Read, Write},
    path::PathBuf,
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
            db.execute(
                "UPDATE conversations SET state='sync_required' WHERE state!='ready'",
                [],
            )?;
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
                    self.error = None;
                    self.source = Some(db);
                }
                Err(_) => {
                    self.source = None;
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
                    json!({"source_available":self.source.is_some(),"session_index_diagnostics":self.session_diagnostics,"image_key_configured":self.image_key.is_some(),"image_config_unavailable":self.image_config_unavailable,"live_sync_ready":false,"sync_mode":"manual","manual_sync_ready":self.source.is_some(),"compatibility":self.error,"index_bytes":bytes,"quota_bytes":quota,"quota_warning":bytes>quota as u64,"conversations":idx.list("",false)?["items"],"archive_semantics":"first_observed"}),
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
                Ok(json!({"approved":true,"conversation_id":id,"state":"sync_required"}))
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
            "sync" => self.manual_sync(p),
            "get_updates" => {
                if p["wait_seconds"].as_i64().unwrap_or(0) != 0 {
                    bail!("manual_sync_required: wait_seconds is no longer supported; call wechat_sync first");
                }
                let mut value = idx.query(p, true)?;
                let conv = idx.resolve(&text(p, "conversation"))?;
                let synced: Option<String> = idx
                    .db
                    .query_row(
                        "SELECT value FROM settings WHERE key=?1",
                        [format!("synced:{conv}")],
                        |r| r.get(0),
                    )
                    .optional()?;
                value["sync_mode"] = json!("manual");
                value["last_synced_at"] = json!(synced);
                value["source_checked"] = json!(false);
                value["sync_required"] = json!(true);
                Ok(value)
            }
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
    fn manual_sync(&mut self, p: &Value) -> Result<Value> {
        let idx = self.index.as_ref().context("setup_required")?;
        let source = self.source.as_ref().context("source_unavailable")?;
        let conv = idx.resolve(&text(p, "conversation"))?;
        let started = chrono::Utc::now().to_rfc3339();
        let snapshots = source.history_snapshots()?;
        if snapshots.is_empty() {
            bail!("source_shards_unavailable");
        }
        // One archive transaction: errors never advance a cursor or publish partial results.
        let tx = idx.db.unchecked_transaction()?;
        let mut processed = 0usize;
        for snapshot in snapshots {
            let shard = snapshot.shard_id();
            let position: Option<String> = tx
                .query_row(
                    "SELECT position FROM import_positions WHERE conversation=?1 AND shard=?2",
                    params![conv, shard],
                    |r| r.get(0),
                )
                .optional()?;
            let mut position = position
                .map(|s| s.parse::<i64>())
                .transpose()?
                .unwrap_or(i64::MIN);
            loop {
                let (batch, last) = snapshot.page(&conv, position, 128)?;
                for message in &batch {
                    idx.insert(message)?;
                }
                processed += batch.len();
                position = last;
                if batch.len() < 128 {
                    break;
                }
            }
            tx.execute("INSERT INTO import_positions VALUES(?1,?2,?3,1) ON CONFLICT(conversation,shard) DO UPDATE SET position=excluded.position,done=1",
                params![conv, shard, position.to_string()])?;
        }
        tx.execute(
            "UPDATE conversations SET state='ready' WHERE id=?1",
            [&conv],
        )?;
        tx.execute("INSERT INTO settings VALUES(?1,?2) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            params![format!("synced:{conv}"), started])?;
        tx.commit()?;
        Ok(
            json!({"conversation_id":conv,"sync_mode":"manual","snapshot_started_at":started,
            "processed_messages":processed,"index_state":"ready","archive_semantics":"first_observed"}),
        )
    }
}

fn run() -> Result<()> {
    #[cfg(unix)]
    unsafe {
        libc_umask();
    }
    let mut backend = Backend::new();
    let mut input = std::io::stdin().lock();
    let mut output = std::io::stdout().lock();
    while let Some(v) = read_frame(&mut input)? {
        let id = v["id"].clone();
        let response = match backend.request(&text(&v, "method"), &v["params"]) {
            Ok(result) => json!({"jsonrpc":"2.0","id":id,"result":result}),
            Err(e) => {
                json!({"jsonrpc":"2.0","id":id,"error":{"code":-32000,"message":e.to_string()}})
            }
        };
        write_frame(&mut output, &response)?;
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
    fn source_fixture() -> (tempfile::TempDir, Backend, rusqlite::Connection) {
        let d = tempfile::tempdir().unwrap();
        for directory in ["contact", "session", "message"] {
            std::fs::create_dir(d.path().join(directory)).unwrap();
        }
        rusqlite::Connection::open(d.path().join("contact/contact.db")).unwrap();
        rusqlite::Connection::open(d.path().join("session/session.db")).unwrap();
        let c = rusqlite::Connection::open(d.path().join("message/message_0.db")).unwrap();
        // md5("g") is the source conversation table suffix.
        c.execute_batch("PRAGMA journal_mode=WAL;
            CREATE TABLE Name2Id(user_name TEXT); INSERT INTO Name2Id VALUES('a');
            CREATE TABLE Msg_b2f5ff47436671b6e533d8dc3614845d(local_id INTEGER PRIMARY KEY,sort_seq INTEGER,create_time INTEGER,server_id INTEGER,local_type INTEGER,real_sender_id INTEGER,message_content BLOB,packed_info_data BLOB,status INTEGER);").unwrap();
        let i = Index::open(&d.path().join("archive.db"), "a").unwrap();
        i.grant("g", "群").unwrap();
        let mut backend = Backend::new();
        backend.source = Some(WechatDb::open(d.path()).unwrap());
        backend.index = Some(i);
        (d, backend, c)
    }
    fn source_add(c: &rusqlite::Connection, id: i64, time: i64) {
        c.execute("INSERT INTO Msg_b2f5ff47436671b6e533d8dc3614845d VALUES(?1,?2,?2,?1,1,1,'hello',NULL,0)", params![id,time]).unwrap();
    }
    #[test]
    fn manual_sync_delta_restart_and_late_timestamps() {
        let (d, mut b, c) = source_fixture();
        for id in 1..=260 {
            source_add(&c, id, 1700000000 + id);
        }
        let p = json!({"conversation":"g"});
        assert_eq!(b.request("sync", &p).unwrap()["processed_messages"], 260);
        let updates = b
            .request("get_updates", &json!({"conversation":"g","limit":500}))
            .unwrap();
        assert_eq!(updates["items"].as_array().unwrap().len(), 260);
        assert_eq!(updates["source_checked"], false);
        assert_eq!(b.request("sync", &p).unwrap()["processed_messages"], 0);
        source_add(&c, 261, 1); // A newly appended row can have an old timestamp.
        let cursor = json!({"conversation":"g","limit":500,"cursor":updates["next_cursor"]});
        assert!(b.request("get_updates", &cursor).unwrap()["items"]
            .as_array()
            .unwrap()
            .is_empty());
        b.index = None;
        b.index = Some(Index::open(&d.path().join("archive.db"), "a").unwrap());
        assert_eq!(b.request("sync", &p).unwrap()["processed_messages"], 1);
        assert_eq!(
            b.request("get_updates", &cursor).unwrap()["items"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert!(b
            .request("get_updates", &json!({"conversation":"g","wait_seconds":1}))
            .is_err());
        b.index.as_ref().unwrap().revoke("g", false).unwrap();
        assert!(b.request("sync", &p).is_err());
    }
    #[test]
    fn sync_error_rolls_back_messages_and_positions() {
        let (_d, mut b, c) = source_fixture();
        source_add(&c, 1, 1);
        b.request("sync", &json!({"conversation":"g"})).unwrap();
        for id in 2..=130 {
            source_add(&c, id, id);
        }
        c.execute(
            "UPDATE Msg_b2f5ff47436671b6e533d8dc3614845d SET sort_seq=NULL WHERE local_id=130",
            [],
        )
        .unwrap();
        assert!(b.request("sync", &json!({"conversation":"g"})).is_err());
        let idx = b.index.as_ref().unwrap();
        let count: i64 = idx
            .db
            .query_row("SELECT count(*) FROM messages", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1);
        let position: String = idx
            .db
            .query_row("SELECT position FROM import_positions", [], |r| r.get(0))
            .unwrap();
        assert_eq!(position, "1");
        c.execute(
            "UPDATE Msg_b2f5ff47436671b6e533d8dc3614845d SET sort_seq=130 WHERE local_id=130",
            [],
        )
        .unwrap();
        assert_eq!(
            b.request("sync", &json!({"conversation":"g"})).unwrap()["processed_messages"],
            129
        );
    }
    #[test]
    fn snapshot_defers_concurrent_writes_and_discovers_new_shards() {
        let (d, mut b, c) = source_fixture();
        source_add(&c, 1, 1);
        let source = b.source.as_ref().unwrap();
        let snapshots = source.history_snapshots().unwrap();
        source_add(&c, 2, 2);
        let (batch, position) = snapshots[0].page("g", i64::MIN, 128).unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(position, 1);
        assert!(snapshots[0].page("g", position, 128).unwrap().0.is_empty());
        drop(snapshots);
        assert_eq!(
            source.history_snapshots().unwrap()[0]
                .page("g", position, 128)
                .unwrap()
                .0
                .len(),
            1
        );
        let rotated = rusqlite::Connection::open(d.path().join("message/message_1.db")).unwrap();
        let ddl: String = c
            .query_row(
                "SELECT sql FROM sqlite_master WHERE name='Msg_b2f5ff47436671b6e533d8dc3614845d'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        rotated.execute_batch(&ddl).unwrap();
        rotated
            .execute_batch("CREATE TABLE Name2Id(user_name TEXT); INSERT INTO Name2Id VALUES('a');")
            .unwrap();
        source_add(&rotated, 3, 0);
        assert_eq!(source.history_snapshots().unwrap().len(), 2);
        assert_eq!(
            b.request("sync", &json!({"conversation":"g"})).unwrap()["processed_messages"],
            3
        );
        assert_eq!(
            b.request("sync", &json!({"conversation":"g"})).unwrap()["processed_messages"],
            0
        );
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
