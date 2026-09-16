//! Bounded, indexed primitives for consumers that must never silently full-scan.
//! Session watermarks are hints, not a durable change log.
use crate::decode::{check_column_exists, decode_message_for_test, msg_table_name};
use crate::{DbError, Message, WechatDb};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct MessagePosition {
    pub sort_seq: i64,
    pub create_time: i64,
    pub server_id: i64,
    pub local_id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceMessage {
    pub position: MessagePosition,
    pub shard_id: String,
    pub message: Message,
}

/// A read transaction pinned at manual-sync start. Dropping it releases the WAL snapshot.
pub struct HistorySnapshot {
    conn: Connection,
    shard: String,
}
impl HistorySnapshot {
    pub fn shard_id(&self) -> &str {
        &self.shard
    }
    pub fn page(
        &self,
        talker: &str,
        after: i64,
        limit: usize,
    ) -> Result<(Vec<SourceMessage>, i64), DbError> {
        read_source_connection(
            &self.conn,
            &self.shard,
            talker,
            &MessagePosition::default(),
            limit,
            Some(after),
        )
    }
}

/// Verify an actual range SEARCH, not merely a covering-index full SCAN.
pub fn require_range_search(
    conn: &Connection,
    sql: &str,
    values: &[&dyn rusqlite::ToSql],
) -> Result<(), DbError> {
    let mut stmt = conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}"))?;
    let plans = stmt
        .query_map(values, |r| r.get::<_, String>(3))?
        .collect::<Result<Vec<_>, _>>()?;
    if !plans
        .iter()
        .any(|p| p.contains("SEARCH") && (p.contains("INDEX") || p.contains("PRIMARY KEY")))
        || plans
            .iter()
            .any(|p| p.contains("SCAN ") || p.contains("TEMP B-TREE"))
    {
        return Err(DbError::FtsInit(
            "incompatible_source_index: bounded range index required".into(),
        ));
    }
    Ok(())
}

impl WechatDb {
    /// Schema/query-plan metadata only. Does not execute the history query.
    pub fn session_index_diagnostics(&self) -> Result<Vec<String>, DbError> {
        let mut out = Vec::new();
        let mut indexes = self
            .session_conn
            .prepare("SELECT name FROM pragma_index_list('SessionTable')")?;
        for name in indexes.query_map([], |r| r.get::<_, String>(0))? {
            let name = name?;
            let mut columns = self
                .session_conn
                .prepare("SELECT name FROM pragma_index_info(?1) ORDER BY seqno")?;
            let names = columns
                .query_map([&name], |r| r.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?;
            out.push(format!("index {name}: {}", names.join(",")));
        }
        let mut plan=self.session_conn.prepare("EXPLAIN QUERY PLAN SELECT username,sort_timestamp FROM SessionTable WHERE (sort_timestamp,username)>(0,'') ORDER BY sort_timestamp,username LIMIT 128")?;
        for row in plan.query_map([], |r| r.get::<_, String>(3))? {
            out.push(row?);
        }
        Ok(out)
    }
    pub fn session_changes_page(
        &self,
        timestamp: i64,
        username: &str,
        limit: usize,
    ) -> Result<Vec<(String, i64)>, DbError> {
        let sql = "SELECT username,sort_timestamp FROM SessionTable WHERE (sort_timestamp,username) > (?1,?2) ORDER BY sort_timestamp,username LIMIT ?3";
        let cap = limit.clamp(1, 512) as i64;
        require_range_search(&self.session_conn, sql, &[&timestamp, &username, &cap])?;
        let mut stmt = self.session_conn.prepare(sql)?;
        let rows = stmt
            .query_map(params![timestamp, username, cap], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Discovery is startup/explicit-import work only. Never call from a poll.
    pub fn source_shards(&self) -> Vec<String> {
        self.shards
            .iter()
            .filter_map(|s| s.path.file_name()?.to_str().map(str::to_owned))
            .collect()
    }

    /// Explicit discovery reads filenames only, including newly rotated shards.
    /// Pin all shard snapshots before reading any message pages.
    pub fn history_snapshots(&self) -> Result<Vec<HistorySnapshot>, DbError> {
        let directory = self
            .session_path
            .parent()
            .and_then(|p| p.parent())
            .ok_or(DbError::NoShards)?
            .join("message");
        let paths = std::fs::read_dir(directory)?.collect::<Result<Vec<_>, _>>()?;
        let mut snapshots = Vec::new();
        for entry in paths {
            let shard = entry.file_name().to_string_lossy().into_owned();
            let Some(number) = shard
                .strip_prefix("message_")
                .and_then(|s| s.strip_suffix(".db"))
            else {
                continue;
            };
            if number.is_empty() || !number.bytes().all(|b| b.is_ascii_digit()) {
                continue;
            }
            let conn = self.open_related_readonly(&entry.path())?;
            conn.execute_batch("BEGIN")?;
            // BEGIN is deferred: force a read to establish the snapshot now.
            conn.query_row("SELECT rootpage FROM sqlite_master LIMIT 1", [], |_| Ok(()))
                .optional()?;
            snapshots.push(HistorySnapshot { conn, shard });
        }
        Ok(snapshots)
    }

    /// Fetch a bounded page from one explicitly selected shard. Stable local ID
    /// prevents collisions for unsent messages whose server_id is zero.
    pub fn source_page(
        &self,
        shard_id: &str,
        talker: &str,
        after: &MessagePosition,
        limit: usize,
    ) -> Result<Vec<SourceMessage>, DbError> {
        self.read_source_page(shard_id, talker, after, limit, None)
            .map(|p| p.0)
    }

    /// Initial historical import only: rowid keyset pagination does not require
    /// a sort-sequence index. Never use this to discover live changes.
    pub fn history_page(
        &self,
        shard_id: &str,
        talker: &str,
        after_rowid: i64,
        limit: usize,
    ) -> Result<(Vec<SourceMessage>, i64), DbError> {
        self.read_source_page(
            shard_id,
            talker,
            &MessagePosition::default(),
            limit,
            Some(after_rowid),
        )
    }

    fn read_source_page(
        &self,
        shard_id: &str,
        talker: &str,
        after: &MessagePosition,
        limit: usize,
        history: Option<i64>,
    ) -> Result<(Vec<SourceMessage>, i64), DbError> {
        if !shard_id.starts_with("message_")
            || !shard_id.ends_with(".db")
            || shard_id.contains('/')
            || shard_id.contains('\\')
        {
            return Err(DbError::NotFound("invalid shard identifier".into()));
        }
        let path = self
            .session_path
            .parent()
            .and_then(|p| p.parent())
            .ok_or(DbError::NoShards)?
            .join("message")
            .join(shard_id);
        let conn = self.open_related_readonly(&path)?;
        read_source_connection(&conn, shard_id, talker, after, limit, history)
    }
}

fn read_source_connection(
    conn: &Connection,
    shard_id: &str,
    talker: &str,
    after: &MessagePosition,
    limit: usize,
    history: Option<i64>,
) -> Result<(Vec<SourceMessage>, i64), DbError> {
    let table = msg_table_name(talker);
    let exists: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1)",
        [&table],
        |r| r.get(0),
    )?;
    if !exists {
        return Ok((vec![], history.unwrap_or(0)));
    }
    let ct = if check_column_exists(&conn, &table, "WCDB_CT_message_content")? {
        "m.WCDB_CT_message_content"
    } else {
        "NULL"
    };
    let compressed = if check_column_exists(&conn, &table, "compress_content")? {
        "m.compress_content"
    } else {
        "NULL"
    };
    let predicate = if history.is_some() {
        "m.rowid>?1 ORDER BY m.rowid LIMIT ?2"
    } else {
        "(m.sort_seq,m.create_time,m.server_id,m.local_id)>(?1,?2,?3,?4) ORDER BY m.sort_seq,m.create_time,m.server_id,m.local_id LIMIT ?5"
    };
    let sql = format!("SELECT m.sort_seq,m.create_time,m.server_id,m.local_id,m.local_type,COALESCE(n.user_name,''),m.message_content,m.packed_info_data,m.status,{ct},{compressed},m.rowid FROM [{table}] m LEFT JOIN Name2Id n ON m.real_sender_id=n.rowid WHERE {predicate}");
    let cap = limit.clamp(1, 512) as i64;
    let rowid = history.unwrap_or(0);
    let values: Vec<&dyn rusqlite::ToSql> = if history.is_some() {
        vec![&rowid, &cap]
    } else {
        vec![
            &after.sort_seq,
            &after.create_time,
            &after.server_id,
            &after.local_id,
            &cap,
        ]
    };
    require_range_search(&conn, &sql, &values)?;
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query(values.as_slice())?;
    let mut out = Vec::new();
    let mut last_rowid = rowid;
    while let Some(r) = rows.next()? {
        last_rowid = r.get(11)?;
        let position = MessagePosition {
            sort_seq: r.get(0)?,
            create_time: r.get(1)?,
            server_id: r.get(2)?,
            local_id: r.get(3)?,
        };
        let content = match r.get_ref(6)? {
            rusqlite::types::ValueRef::Blob(b) | rusqlite::types::ValueRef::Text(b) => b.to_vec(),
            _ => Vec::new(),
        };
        let optional_bytes = |column| -> Result<Option<Vec<u8>>, rusqlite::Error> {
            Ok(match r.get_ref(column)? {
                rusqlite::types::ValueRef::Blob(b) | rusqlite::types::ValueRef::Text(b)
                    if !b.is_empty() =>
                {
                    Some(b.to_vec())
                }
                _ => None,
            })
        };
        let packed = optional_bytes(7)?;
        let compressed = optional_bytes(10)?;
        let message = decode_message_for_test(
            position.sort_seq,
            position.server_id,
            r.get(4)?,
            &r.get::<_, String>(5)?,
            talker,
            position.create_time,
            &content,
            packed.as_deref(),
            r.get::<_, Option<i32>>(8)?.unwrap_or(0),
            r.get(9)?,
            compressed.as_deref(),
            talker.ends_with("@chatroom"),
        )?;
        out.push(SourceMessage {
            position,
            shard_id: shard_id.to_owned(),
            message,
        });
    }
    Ok((out, last_rowid))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn history_pages_do_not_need_sort_index() {
        let dir = tempfile::tempdir().unwrap();
        for name in ["contact", "session", "message"] {
            std::fs::create_dir(dir.path().join(name)).unwrap();
        }
        let contact = Connection::open(dir.path().join("contact/contact.db")).unwrap();
        crate::test_ddl::create_test_contact_table_minimal(&contact);
        let session = Connection::open(dir.path().join("session/session.db")).unwrap();
        crate::test_ddl::create_test_session_table(&session);
        let conn = Connection::open(dir.path().join("message/message_0.db")).unwrap();
        let table = msg_table_name("wxid_alice");
        conn.execute_batch(&format!("CREATE TABLE Timestamp(timestamp INTEGER); INSERT INTO Timestamp VALUES(1);
            CREATE TABLE Name2Id(user_name TEXT); INSERT INTO Name2Id VALUES('wxid_alice');
            CREATE TABLE [{table}](local_id INTEGER PRIMARY KEY,sort_seq INTEGER,create_time INTEGER,server_id INTEGER,local_type INTEGER,real_sender_id INTEGER,message_content BLOB,packed_info_data BLOB,status INTEGER);
            INSERT INTO [{table}] VALUES(2,10,1,100,1,1,X'6869',NULL,0),(7,10,1,101,1,1,X'6869',NULL,0),(20,1,1,102,1,1,X'6869',NULL,0);" )).unwrap();
        conn.execute_batch(&format!("ALTER TABLE [{table}] ADD COLUMN compress_content BLOB; UPDATE [{table}] SET packed_info_data='',compress_content='';")).unwrap();
        let db = WechatDb::open(dir.path()).unwrap();
        assert!(db
            .source_page("message_0.db", "wxid_alice", &MessagePosition::default(), 2)
            .is_err());
        let (first, cursor) = db
            .history_page("message_0.db", "wxid_alice", i64::MIN, 2)
            .unwrap();
        assert_eq!(first.len(), 2);
        assert_eq!(cursor, 7);
        let (last, cursor) = db
            .history_page("message_0.db", "wxid_alice", cursor, 2)
            .unwrap();
        assert_eq!(last.len(), 1);
        assert_eq!(last[0].position.local_id, 20);
        assert_eq!(cursor, 20);
        assert!(db
            .history_page("message_0.db", "wxid_alice", cursor, 2)
            .unwrap()
            .0
            .is_empty());
    }
    #[test]
    fn rejects_scan_accepts_range() {
        let c = Connection::open_in_memory().unwrap();
        c.execute_batch("CREATE TABLE SessionTable(username TEXT,sort_timestamp INTEGER)")
            .unwrap();
        let sql="SELECT username,sort_timestamp FROM SessionTable WHERE (sort_timestamp,username)>(?1,?2) ORDER BY sort_timestamp,username LIMIT 100";
        assert!(require_range_search(&c, sql, &[&0, &""]).is_err());
        c.execute_batch("CREATE INDEX session_range ON SessionTable(sort_timestamp,username)")
            .unwrap();
        assert!(require_range_search(&c, sql, &[&0, &""]).is_ok());
    }
}
