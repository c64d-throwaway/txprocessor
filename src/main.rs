use anyhow::{Context, Result};
use rusqlite::{
    params, types::ToSqlOutput, Connection as SqlConnection, Error as SqlError, OptionalExtension,
    Result as SqlResult, ToSql,
};
use serde::{de, Deserialize, Deserializer};
use serde_derive::Deserialize as SerdeDeserialize;
use std::collections::{HashMap, VecDeque};
use std::fs::OpenOptions;
use std::path::PathBuf;
use rusqlite::types::{FromSql, FromSqlError, FromSqlResult, ValueRef};
use strum_macros::{Display, EnumString};
use crate::TxStatus::{InDispute, Processed};

type ClientId = u16;
type TxId = u32;
type Amount = f64;

#[derive(Debug, SerdeDeserialize)]
struct Tx {
    #[serde(rename(deserialize = "tx"))]
    pub id: TxId,
    #[serde(rename(deserialize = "type"))]
    pub tx_type: TxType,
    #[serde(rename(deserialize = "client"))]
    pub client_id: ClientId,
    // FIXME
    pub amount: String,
}

#[derive(Debug, SerdeDeserialize)]
struct SqlTx {
    pub id: TxId,
    pub tx_type: TxType,
    pub client_id: ClientId,
    // FIXME
    pub amount: f64,
    pub status: TxStatus,
}

#[derive(Debug, EnumString, Display)]
enum TxType {
    #[strum(to_string = "deposit", serialize = "deposit")]
    Deposit,
    #[strum(to_string = "withdrawal", serialize = "withdrawal")]
    Withdrawal,
    #[strum(to_string = "dispute", serialize = "dispute")]
    Dispute,
    #[strum(to_string = "resolve", serialize = "resolve")]
    Resolve,
    #[strum(to_string = "chargeback", serialize = "chargeback")]
    Chargeback,
}

impl<'de> Deserialize<'de> for TxType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
    {
        let s: &str = Deserialize::deserialize(deserializer)?;

        match s {
            "deposit" => Ok(TxType::Deposit),
            "withdrawal" => Ok(TxType::Withdrawal),
            "dispute" => Ok(TxType::Dispute),
            "resolve" => Ok(TxType::Resolve),
            "chargeback" => Ok(TxType::Chargeback),
            _ => Err(de::Error::custom(format!(
                "{} is an invalid transaction type",
                s
            ))),
        }
    }
}

impl ToSql for TxType {
    fn to_sql(&self) -> SqlResult<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::from(self.to_string()))
    }
}


#[derive(Debug, EnumString, Display)]
enum TxStatus {
    #[strum(to_string = "processed", serialize = "processed")]
    Processed,
    #[strum(to_string = "in_dispute", serialize = "in_dispute")]
    InDispute,
    #[strum(to_string = "resolved", serialize = "resolved")]
    Resolved,
    #[strum(to_string = "chargeback", serialize = "chargeback")]
    Chargeback,
}

impl<'de> Deserialize<'de> for TxStatus {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
    {
        let s: &str = Deserialize::deserialize(deserializer)?;

        match s {
            "processed" => Ok(TxStatus::Processed),
            "in_dispute" => Ok(TxStatus::InDispute),
            "resolved" => Ok(TxStatus::Resolved),
            "chargeback" => Ok(TxStatus::Chargeback),
            _ => Err(de::Error::custom(format!(
                "{} is an invalid transaction status",
                s
            ))),
        }
    }
}

impl ToSql for TxStatus {
    fn to_sql(&self) -> SqlResult<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::from(self.to_string()))
    }
}

impl FromSql for TxStatus {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value.as_str()? {
            "processed" => Ok(TxStatus::Processed),
            "in_dispute" => Ok(TxStatus::InDispute),
            "resolved" => Ok(TxStatus::Resolved),
            _ => Err(FromSqlError::from(FromSqlError::InvalidType)),
        }
    }
}


#[derive(Debug, EnumString, Display)]
enum AccountStatus {
    #[strum(to_string = "active", serialize = "active")]
    Active,
    #[strum(to_string = "blocked", serialize = "blocked")]
    Blocked,
    #[strum(to_string = "inactive", serialize = "inactive")]
    Inactive,
}

impl ToSql for AccountStatus {
    fn to_sql(&self) -> SqlResult<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::from(self.to_string()))
    }
}

impl FromSql for TxSAccountStatustatus {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value.as_str()? {
            "active" => Ok(TxStatus::Processed),
            "blocked" => Ok(TxStatus::InDispute),
            "inactive" => Ok(TxStatus::Resolved),
            _ => Err(FromSqlError::from(FromSqlError::InvalidType)),
        }
    }
}

struct Account {
    pub client_id: ClientId,
    pub available: Amount,
    pub held: Amount,
    pub locked: bool,
}

impl Account {
    fn new(client_id: ClientId) -> Self {
        Account {
            client_id,
            available: 0f64,
            held: 0f64,
            locked: false,
        }
    }

    fn into_csv() {}
}

fn read_csv(path: PathBuf) -> Result<Vec<Tx>> {
    let a = OpenOptions::new().read(true).open(path).unwrap();
    let mut b = csv::Reader::from_reader(a);
    b.deserialize()
        .map(|x| {
            let tx: Tx = x.context("failed deserializing csv record into a transaction")?;
            Ok(tx)
        })
        .collect::<Result<_>>()
}

struct TxQueue {
    q: VecDeque<Tx>,
}

impl TxQueue {
    pub fn new() -> Self {
        TxQueue { q: VecDeque::new() }
    }

    pub fn push(&mut self, tx: Tx) {
        self.q.push_back(tx);
    }

    pub fn pop(&mut self) -> Option<Tx> {
        self.q.pop_front()
    }

    pub fn len(&self) -> usize {
        self.q.len()
    }
}

fn insert_tx(conn: &SqlConnection, tx: &Tx) -> Result<()> {
    conn.execute(
        "INSERT INTO tx (id, tx_type, client_id, amount, status) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![tx.id, tx.tx_type.to_string(), tx.client_id, tx.amount],
    )
        .context("unable to insert tx to table")?;

    Ok(())
}

fn handle_tx(conn: &mut SqlConnection, tx: Tx) -> Result<()> {
    let dbtx = conn.transaction()?;

    let a = match tx.tx_type {
        TxType::Deposit => {
            let num_of_records: i64 = dbtx.query_row(
                "SELECT count(id) FROM tx where id = ?1",
                params![&tx.id],
                |row| Ok(row.get(0)?),
            )?;
            if num_of_records == 1 {
                dbtx.rollback()?;
                return Ok(());
            }

            dbtx.execute(
                "UPDATE account SET available_amount = available_amount + ?1 WHERE id = ?2;",
                params![tx.amount, tx.client_id])
                .map(|x| ())
                .and_then(|_|
                    dbtx.execute(
                        "INSERT OR IGNORE INTO tx (id, tx_type, client_id, amount) values (?1, ?2, ?3, ?4);",
                        params![tx.id, tx.tx_type, tx.client_id, tx.amount])
                        .map(|x| ()))
        }

        TxType::Withdrawal => {
            let num_of_records: i64 = dbtx.query_row(
                "SELECT count(id) FROM tx where id = ?1",
                params![&tx.id],
                |row| Ok(row.get(0)?),
            )?;
            if num_of_records == 1 {
                dbtx.rollback()?;
                return Ok(());
            }

            dbtx.execute(
                "UPDATE account SET available_amount = available_amount - ?1 WHERE id = ?2 AND available_amount >= ?1;",
                params![tx.amount, tx.client_id])
                .map(|x| ())
                .and_then(|_|
                    dbtx.execute(
                        "INSERT OR IGNORE INTO tx (id, tx_type, client_id, amount) values (?1, ?2, ?3, ?4);",
                        params![tx.id, tx.tx_type, tx.client_id, tx.amount])
                        .map(|x| ()))
        }

        TxType::Dispute => {
            let txrecord = dbtx.query_row(
                "select id, tx_type, client_id, amount, status from tx where status = ?3 and client_id = ?1 and id = ?2;",
                params![&tx.client_id, &tx.id, TxStatus::Processed.to_string()], |r| {
                    let id: u32 = r.get(0)?;
                    Ok(SqlTx {
                        id,
                        tx_type: TxType::Deposit, // fixme!!!!
                        client_id: r.get(2)?,
                        amount: r.get(3)?,
                        status: r.get(4)?,
                    })
                }).unwrap();


            dbtx.execute(
                "UPDATE tx SET status = ?2 WHERE id = ?1;",
                params![&txrecord.id, TxStatus::InDispute],
            ).map(|x| ()).unwrap();

            dbtx.execute(
                "UPDATE account SET available_amount = available_amount - ?1, held_amount = held_amount + ?1 WHERE id = ?2;",
                params![txrecord.amount, txrecord.client_id],
            ).map(|x| ()).unwrap();

            Ok(())
        }
        TxType::Resolve => {
            let txrecord = dbtx.query_row(
                "select id, tx_type, client_id, amount, status from tx where status = ?3 and client_id = ?1 and id = ?2;",
                params![&tx.client_id, &tx.id, TxStatus::InDispute.to_string()], |r| {
                    let id: u32 = r.get(0)?;
                    Ok(SqlTx {
                        id,
                        tx_type: TxType::Deposit, // fixme!!!!
                        client_id: r.get(2)?,
                        amount: r.get(3)?,
                        status: r.get(4)?,
                    })
                }).unwrap();


            dbtx.execute(
                "UPDATE tx SET status = ?2 WHERE id = ?1;",
                params![&txrecord.id, TxStatus::Resolved],
            ).map(|x| ()).unwrap();

            dbtx.execute(
                "UPDATE account SET available_amount = available_amount + ?1, held_amount = held_amount - ?1 WHERE id = ?2;",
                params![txrecord.amount, txrecord.client_id],
            ).map(|x| ()).unwrap();

            Ok(())
        }
        TxType::Chargeback => {
            let txrecord = dbtx.query_row(
                "select id, tx_type, client_id, amount, status from tx where status = ?3 and client_id = ?1 and id = ?2;",
                params![&tx.client_id, &tx.id, TxStatus::InDispute.to_string()], |r| {
                    let id: u32 = r.get(0)?;
                    Ok(SqlTx {
                        id,
                        tx_type: TxType::Deposit, // fixme!!!!
                        client_id: r.get(2)?,
                        amount: r.get(3)?,
                        status: r.get(4)?,
                    })
                }).unwrap();


            dbtx.execute(
                "UPDATE tx SET status = ?2 WHERE id = ?1;",
                params![&txrecord.id, TxStatus::Chargeback],
            ).map(|x| ()).unwrap();

            // fix block the fucking user
            dbtx.execute(
                "UPDATE account SET held_amount = held_amount - ?1, status = ?2 WHERE id = ?3;",
                params![txrecord.amount, AccountStatus::Blocked, txrecord.client_id],
            ).map(|x| ()).unwrap();

            Ok(())
        }
    };
    a.unwrap();
    dbtx.commit()?;
    Ok(())
}

fn main() -> Result<()> {
    let mut conn = SqlConnection::open("test.db")?;
    let txs = read_csv(PathBuf::from("./src/testfile.csv")).unwrap();
    let mut queue = TxQueue::new();

    conn.execute("CREATE TABLE IF NOT EXISTS tx (id INTEGER PRIMARY KEY, tx_type TEXT, client_id INTEGER, amount DOUBLE PRECISION, status TEXT DEFAULT ?1);", params![TxStatus::Processed]).unwrap();
    conn.execute("CREATE TABLE IF NOT EXISTS account (id INTEGER PRIMARY KEY, available_amount DOUBLE PRECISION , held_amount DOUBLE PRECISION, locked BOOLEAN, status TEXT DEFAULT 'active');", []).unwrap();
    conn.execute("INSERT OR IGNORE INTO account (id, available_amount, held_amount, locked) values (1, 0.0, 0.0, false)", []).unwrap();
    conn.execute("INSERT OR IGNORE INTO account (id, available_amount, held_amount, locked) values (2, 0.0, 0.0, false)", []).unwrap();
    for tx in txs {
        queue.push(tx);
    }

    while let Some(tx) = queue.pop() {
        handle_tx(&mut conn, tx);
    }

    Ok(())
}


// TODO
// 1. add status to account and block upon chargeback
// 2. split actions to functions
// 3. serialize the account as CSV
// 4. run queue on own process?
// 5. shard the queue and database?
