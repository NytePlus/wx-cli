use anyhow::{bail, Context, Result};
use chrono::DateTime;
use hmac::{Hmac, Mac};
use rusqlite::{params, params_from_iter, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::path::Path;
use wx_db::{incremental::SourceMessage, MessageContent};

pub struct Index {
    pub db: Connection,
    secret: Vec<u8>,
    account: String,
}

#[derive(Serialize, Deserialize)]
struct Cursor {
    account: String,
    generation: i64,
    scope: String,
    snapshot: i64,
    time: i64,
    id: String,
}

pub fn text(p: &Value, key: &str) -> String {
    p[key].as_str().unwrap_or("").to_owned()
}
fn tokens(s: &str) -> String {
    let mut out = String::new();
    for ch in s.chars() {
        if ('\u{3400}'..='\u{9fff}').contains(&ch) {
            out.push(' ');
            out.push(ch);
            out.push(' ');
        } else {
            out.push(ch);
        }
    }
    out
}
fn match_query(s: &str) -> String {
    s.split_whitespace()
        .map(|s| format!("\"{}\"", tokens(s).trim().replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(" AND ")
}

impl Index {
    pub fn open(path: &Path, account: &str) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let db = Connection::open(path)?;
        db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON; PRAGMA secure_delete=ON;
          CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY,value TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS conversations(id TEXT PRIMARY KEY,name TEXT NOT NULL,enabled INTEGER NOT NULL,state TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS messages(seq INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT UNIQUE NOT NULL,conversation TEXT NOT NULL,member TEXT NOT NULL,time INTEGER NOT NULL,sort_seq INTEGER NOT NULL,type TEXT NOT NULL,body TEXT NOT NULL,fts_body TEXT NOT NULL,data TEXT NOT NULL);
          CREATE INDEX IF NOT EXISTS by_time ON messages(conversation,time,id);
          CREATE INDEX IF NOT EXISTS by_member ON messages(conversation,member,time,id);
          CREATE INDEX IF NOT EXISTS by_type ON messages(conversation,type,time,id);
          CREATE INDEX IF NOT EXISTS by_sort ON messages(conversation,sort_seq,id);
          CREATE TABLE IF NOT EXISTS members(conversation TEXT, id TEXT, name TEXT, PRIMARY KEY(conversation,id,name));
          CREATE TABLE IF NOT EXISTS import_positions(conversation TEXT,shard TEXT,position TEXT NOT NULL,done INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(conversation,shard));
          CREATE VIRTUAL TABLE IF NOT EXISTS message_fts USING fts5(fts_body,content='messages',content_rowid='seq');
          CREATE TRIGGER IF NOT EXISTS messages_ai AFTER INSERT ON messages BEGIN INSERT INTO message_fts(rowid,fts_body) VALUES(new.seq,new.fts_body); END;
          CREATE TRIGGER IF NOT EXISTS messages_ad AFTER DELETE ON messages BEGIN INSERT INTO message_fts(message_fts,rowid,fts_body) VALUES('delete',old.seq,old.fts_body); END;
          INSERT OR IGNORE INTO settings VALUES('generation','0');
          INSERT OR IGNORE INTO settings VALUES('quota','5368709120');")?;
        let stored: Option<String> = db
            .query_row("SELECT value FROM settings WHERE key='account'", [], |r| {
                r.get(0)
            })
            .optional()?;
        if stored.as_deref().is_some_and(|a| a != account) {
            bail!("account_mismatch");
        }
        db.execute(
            "INSERT OR IGNORE INTO settings VALUES('account',?1)",
            [account],
        )?;
        let key: Option<String> = db
            .query_row(
                "SELECT value FROM settings WHERE key='cursor_key'",
                [],
                |r| r.get(0),
            )
            .optional()?;
        let secret = if let Some(k) = key {
            hex::decode(k)?
        } else {
            let mut b = vec![0u8; 32];
            getrandom::getrandom(&mut b).map_err(|_| anyhow::anyhow!("random unavailable"))?;
            db.execute(
                "INSERT INTO settings VALUES('cursor_key',?1)",
                [hex::encode(&b)],
            )?;
            b
        };
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
        }
        Ok(Self {
            db,
            secret,
            account: account.to_owned(),
        })
    }
    fn generation(&self) -> Result<i64> {
        Ok(self.db.query_row(
            "SELECT CAST(value AS INTEGER) FROM settings WHERE key='generation'",
            [],
            |r| r.get(0),
        )?)
    }
    pub fn grant(&self, id: &str, name: &str) -> Result<()> {
        let tx = self.db.unchecked_transaction()?;
        tx.execute("INSERT INTO conversations VALUES(?1,?2,1,'indexing') ON CONFLICT(id) DO UPDATE SET enabled=1,name=excluded.name",params![id,name])?;
        tx.execute(
            "UPDATE settings SET value=CAST(value AS INTEGER)+1 WHERE key='generation'",
            [],
        )?;
        tx.commit()?;
        Ok(())
    }
    pub fn revoke(&self, id: &str, delete: bool) -> Result<()> {
        let tx = self.db.unchecked_transaction()?;
        tx.execute("UPDATE conversations SET enabled=0 WHERE id=?1", [id])?;
        if delete {
            for table in ["messages", "members", "import_positions"] {
                tx.execute(&format!("DELETE FROM {table} WHERE conversation=?1"), [id])?;
            }
            tx.execute("DELETE FROM conversations WHERE id=?1", [id])?;
        }
        tx.execute(
            "UPDATE settings SET value=CAST(value AS INTEGER)+1 WHERE key='generation'",
            [],
        )?;
        tx.commit()?;
        Ok(())
    }
    pub fn resolve(&self, name: &str) -> Result<String> {
        let mut stmt=self.db.prepare("SELECT id FROM conversations WHERE enabled=1 AND (id=?1 OR name=?1) ORDER BY id LIMIT 21")?;
        let ids = stmt
            .query_map([name], |r| r.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        match ids.len() {
            1 => Ok(ids[0].clone()),
            0 => bail!("conversation_unavailable"),
            _ => bail!("ambiguous_conversation: {}", serde_json::to_string(&ids)?),
        }
    }
    pub fn list(&self, q: &str, local: bool) -> Result<Value> {
        let mut stmt=self.db.prepare("SELECT id,name,state,enabled FROM conversations WHERE (enabled=1 OR ?2) AND (instr(name,?1)>0 OR instr(id,?1)>0) ORDER BY name,id LIMIT 200")?;
        let items=stmt.query_map(params![q,local],|r|Ok(json!({"conversation_id":r.get::<_,String>(0)?,"name":r.get::<_,String>(1)?,"index_state":r.get::<_,String>(2)?,"enabled":r.get::<_,bool>(3)?})))?.collect::<Result<Vec<_>,_>>()?;
        Ok(json!({"items":items}))
    }
    pub fn insert(&self, source: &SourceMessage) -> Result<bool> {
        let m = &source.message;
        let id = if m.server_id != 0 {
            format!("{}:{}", m.talker, m.server_id)
        } else {
            format!(
                "{}:{}:{}",
                m.talker, source.shard_id, source.position.local_id
            )
        };
        let (kind, body) = normalize(&m.content);
        let data = json!({"message_id":id,"conversation_id":m.talker,"member_id":m.sender,"create_time":m.create_time,"created_at":DateTime::from_timestamp(m.create_time,0).map(|t|t.to_rfc3339()),"sort_seq":m.sort_seq,"type":kind,"text":body,"content":m.content,"source":{"shard_id":source.shard_id,"local_id":source.position.local_id,"server_id":m.server_id}});
        // The archive preserves the first observed body, including after recall.
        let n=self.db.execute("INSERT OR IGNORE INTO messages(id,conversation,member,time,sort_seq,type,body,fts_body,data) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)",params![id,m.talker,m.sender,m.create_time,m.sort_seq,kind,body,tokens(&body),data.to_string()])?;
        self.db.execute(
            "INSERT OR IGNORE INTO members VALUES(?1,?2,?2)",
            params![m.talker, m.sender],
        )?;
        Ok(n > 0)
    }
    fn seal(&self, c: &Cursor) -> Result<String> {
        let data = serde_json::to_vec(c)?;
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.secret)?;
        mac.update(&data);
        Ok(format!(
            "{}.{}",
            hex::encode(&data),
            hex::encode(mac.finalize().into_bytes())
        ))
    }
    fn unseal(&self, s: &str, scope: &str) -> Result<Cursor> {
        let (payload, sig) = s.split_once('.').context("invalid_cursor")?;
        let data = hex::decode(payload).context("invalid_cursor")?;
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.secret)?;
        mac.update(&data);
        mac.verify_slice(&hex::decode(sig)?)
            .map_err(|_| anyhow::anyhow!("invalid_cursor"))?;
        let c: Cursor = serde_json::from_slice(&data)?;
        if c.account != self.account || c.scope != scope || c.generation != self.generation()? {
            bail!("cursor_scope_changed");
        }
        Ok(c)
    }
    pub fn query(&self, p: &Value, updates: bool) -> Result<Value> {
        let conv = self.resolve(&text(p, "conversation"))?;
        let state: String = self.db.query_row(
            "SELECT state FROM conversations WHERE id=?1",
            [&conv],
            |r| r.get(0),
        )?;
        if state != "ready" {
            return Ok(json!({"state":state,"items":[]}));
        }
        let limit = p["limit"].as_u64().unwrap_or(50).clamp(1, 500) as i64;
        let desc = !updates && text(p, "order") != "asc";
        let mut canonical = p.clone();
        if let Some(o) = canonical.as_object_mut() {
            o.remove("cursor");
            o.remove("limit");
            o.remove("wait_seconds");
        }
        let scope = hex::encode(Sha256::digest(format!("{updates}:{canonical}")));
        let old = if text(p, "cursor").is_empty() {
            None
        } else {
            Some(self.unseal(&text(p, "cursor"), &scope)?)
        };
        let snapshot = if updates {
            i64::MAX
        } else if let Some(c) = &old {
            c.snapshot
        } else {
            self.db
                .query_row("SELECT COALESCE(MAX(seq),0) FROM messages", [], |r| {
                    r.get(0)
                })?
        };
        let mut clauses = vec!["m.conversation=?".to_owned(), "m.seq<=?".into()];
        let mut values = vec![rusqlite::types::Value::Text(conv.clone()), snapshot.into()];
        for (key, op) in [("start", ">="), ("end", "<")] {
            if let Some(s) = p[key].as_str() {
                let t = DateTime::parse_from_rfc3339(s)
                    .context("time_requires_RFC3339_timezone")?
                    .timestamp();
                clauses.push(format!("m.time{op}?"));
                values.push(t.into());
            }
        }
        if p["start"].is_string()
            && p["end"].is_string()
            && DateTime::parse_from_rfc3339(p["start"].as_str().unwrap())?
                >= DateTime::parse_from_rfc3339(p["end"].as_str().unwrap())?
        {
            bail!("invalid_time_range");
        }
        for (key, col) in [("members", "member"), ("types", "type")] {
            if let Some(a) = p[key].as_array().filter(|a| !a.is_empty()) {
                if a.len() > 100 {
                    bail!("too_many_filters");
                }
                let mut resolved = Vec::new();
                for v in a {
                    let s = v.as_str().context("filter_must_be_string")?;
                    if key == "members" {
                        let mut stmt=self.db.prepare("SELECT DISTINCT id FROM members WHERE conversation=?1 AND (id=?2 OR name=?2) LIMIT 21")?;
                        let ids = stmt
                            .query_map(params![conv, s], |r| r.get::<_, String>(0))?
                            .collect::<Result<Vec<_>, _>>()?;
                        if ids.len() > 1 {
                            bail!("ambiguous_member: {}", serde_json::to_string(&ids)?);
                        }
                        resolved.push(ids.first().cloned().unwrap_or_else(|| s.to_owned()));
                    } else {
                        if !TYPES.contains(&s) {
                            bail!("invalid_message_type");
                        }
                        resolved.push(s.to_owned());
                    }
                }
                clauses.push(format!(
                    "m.{col} IN ({})",
                    vec!["?"; resolved.len()].join(",")
                ));
                values.extend(resolved.into_iter().map(Into::into));
            }
        }
        let keyword = text(p, "keyword");
        if !keyword.trim().is_empty() {
            clauses
                .push("m.seq IN (SELECT rowid FROM message_fts WHERE message_fts MATCH ?)".into());
            values.push(match_query(&keyword).into());
        }
        if updates {
            clauses.push("m.seq>?".into());
            values.push(old.as_ref().map(|c| c.time).unwrap_or(0).into());
        } else if let Some(c) = &old {
            clauses.push(format!(
                "(m.time,m.id){}(?,?)",
                if desc { "<" } else { ">" }
            ));
            values.push(c.time.into());
            values.push(c.id.clone().into());
        }
        let order = if updates {
            "m.seq ASC".to_owned()
        } else {
            format!("m.time {0},m.id {0}", if desc { "DESC" } else { "ASC" })
        };
        let sql = format!(
            "SELECT m.data,m.time,m.id,m.seq FROM messages m WHERE {} ORDER BY {order} LIMIT ?",
            clauses.join(" AND ")
        );
        values.push((limit + 1).into());
        let mut stmt = self.db.prepare(&sql)?;
        let mut rows = stmt.query(params_from_iter(values))?;
        let mut items = Vec::new();
        let mut last = None;
        let mut more = false;
        while let Some(r) = rows.next()? {
            if items.len() == limit as usize {
                more = true;
                break;
            }
            items.push(serde_json::from_str::<Value>(&r.get::<_, String>(0)?)?);
            last = Some((
                if updates { r.get(3)? } else { r.get(1)? },
                r.get::<_, String>(2)?,
            ));
        }
        let cursor = if let Some((time, id)) = last {
            Some(self.seal(&Cursor {
                account: self.account.clone(),
                generation: self.generation()?,
                scope,
                snapshot,
                time,
                id,
            })?)
        } else if updates {
            Some(self.seal(&Cursor {
                account: self.account.clone(),
                generation: self.generation()?,
                scope,
                snapshot,
                time: old.as_ref().map(|c| c.time).unwrap_or(0),
                id: String::new(),
            })?)
        } else {
            None
        };
        Ok(
            json!({"items":items,"has_more":more,"next_cursor":cursor,"archive_semantics":"first_observed","index_state":"ready"}),
        )
    }
    pub fn members(&self, p: &Value) -> Result<Value> {
        let conv = self.resolve(&text(p, "conversation"))?;
        let query = text(p, "query");
        let scope = hex::encode(Sha256::digest(format!("members:{}", json!([conv, query]))));
        let old = if text(p, "cursor").is_empty() {
            None
        } else {
            Some(self.unseal(&text(p, "cursor"), &scope)?)
        };
        let snapshot = if let Some(c) = &old {
            c.snapshot
        } else {
            self.db
                .query_row("SELECT COALESCE(MAX(rowid),0) FROM members", [], |r| {
                    r.get(0)
                })?
        };
        let limit = p["limit"].as_u64().unwrap_or(100).clamp(1, 500) as usize;
        let after = old.as_ref().map(|c| c.id.as_str()).unwrap_or("");
        let mut stmt=self.db.prepare("SELECT id FROM members WHERE conversation=?1 AND id>?2 AND rowid<=?3 AND (instr(name,?4)>0 OR instr(id,?4)>0) GROUP BY id ORDER BY id LIMIT ?5")?;
        let ids = stmt
            .query_map(
                params![conv, after, snapshot, query, (limit + 1) as i64],
                |r| r.get::<_, String>(0),
            )?
            .collect::<Result<Vec<_>, _>>()?;
        let more = ids.len() > limit;
        let mut items = Vec::new();
        for id in ids.iter().take(limit) {
            let mut stmt=self.db.prepare("SELECT name FROM members WHERE conversation=?1 AND id=?2 AND rowid<=?3 ORDER BY name")?;
            let names = stmt
                .query_map(params![conv, id, snapshot], |r| r.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?;
            let name = names
                .iter()
                .find(|n| !n.is_empty() && *n != id)
                .unwrap_or(id);
            items.push(json!({"member_id":id,"name":name,"seen_names":names}));
        }
        let next = if more {
            Some(self.seal(&Cursor {
                account: self.account.clone(),
                generation: self.generation()?,
                scope,
                snapshot,
                time: 0,
                id: ids[limit - 1].clone(),
            })?)
        } else {
            None
        };
        Ok(json!({"items":items,"has_more":more,"next_cursor":next}))
    }
}

pub const TYPES: &[&str] = &[
    "text",
    "file",
    "link",
    "image",
    "voice",
    "video",
    "emoji",
    "location",
    "mini_program",
    "merged_messages",
    "quote",
    "transfer",
    "system",
    "revoke",
    "other",
];
fn normalize(c: &MessageContent) -> (&'static str, String) {
    use MessageContent::*;
    let join = |a: &Option<String>, b: &Option<String>| {
        [a.clone().unwrap_or_default(), b.clone().unwrap_or_default()].join(" ")
    };
    match c {
        Text(s) => ("text", s.clone()),
        Image { .. } => ("image", String::new()),
        Voice => ("voice", String::new()),
        Video { .. } | ChannelVideo { .. } => ("video", String::new()),
        Emoji(_) => ("emoji", String::new()),
        Location(s) => ("location", s.clone()),
        System(s) => ("system", s.clone()),
        Revoke(s) => ("revoke", s.clone()),
        Link {
            title, des, url, ..
        } => (
            "link",
            format!("{} {}", join(title, des), url.clone().unwrap_or_default()),
        ),
        File { title, .. } => ("file", title.clone().unwrap_or_default()),
        MiniProgram { title, url, .. } => ("mini_program", join(title, url)),
        MergedMessages { title, .. } => ("merged_messages", title.clone().unwrap_or_default()),
        Quote {
            reply_text,
            refer_content,
            ..
        } => ("quote", join(reply_text, refer_content)),
        Transfer {
            amount_desc,
            pay_memo,
            ..
        } => ("transfer", join(amount_desc, pay_memo)),
        _ => ("other", String::new()),
    }
}

#[cfg(test)]
mod member_tests {
    use super::*;

    #[test]
    fn pagination_groups_aliases_and_binds_scope() {
        let dir = tempfile::tempdir().unwrap();
        let idx = Index::open(&dir.path().join("index.db"), "account").unwrap();
        idx.grant("group", "Group").unwrap();
        for n in 0..1001 {
            let id = format!("member_{n:04}");
            for alias in [&id, "same_name"] {
                idx.db
                    .execute(
                        "INSERT INTO members VALUES(?1,?2,?3)",
                        params!["group", id, alias],
                    )
                    .unwrap();
            }
        }
        let mut p = json!({"conversation":"group","limit":500});
        let first = idx.members(&p).unwrap();
        assert_eq!(first["items"].as_array().unwrap().len(), 500);
        assert_eq!(first["items"][0]["seen_names"].as_array().unwrap().len(), 2);
        p["cursor"] = first["next_cursor"].clone();
        idx.db
            .execute(
                "INSERT INTO members VALUES('group','member_9999','new')",
                [],
            )
            .unwrap();
        let second = idx.members(&p).unwrap();
        assert_eq!(second["items"][0]["member_id"], "member_0500");
        p["cursor"] = second["next_cursor"].clone();
        let last = idx.members(&p).unwrap();
        assert_eq!(last["items"].as_array().unwrap().len(), 1);
        assert_eq!(last["has_more"], false);
        p["query"] = json!("changed");
        assert!(idx.members(&p).is_err());
        p["query"] = json!("");
        idx.revoke("group", false).unwrap();
        assert!(idx.members(&p).is_err());
    }
}
