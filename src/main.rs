use crate::TxStatus::{InDispute, Processed};
use anyhow::{anyhow, Context, Result};
use rusqlite::{
    params, types::ToSqlOutput, Connection as SqlConnection, Error as SqlError, OptionalExtension,
    Result as SqlResult, ToSql,
};
use rusqlite::{
    types::{FromSql, FromSqlError, FromSqlResult, ValueRef},
    Transaction as SqlTransaction,
};
use serde::{de, Deserialize, Deserializer};
use serde_derive::{Deserialize as SerdeDeserialize, Serialize as SerdeSerialize};
use std::collections::{HashMap, VecDeque};
use std::fs::OpenOptions;
use std::path::PathBuf;
use strum_macros::{Display, EnumString};
use std::io::Write;

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

impl FromSql for TxType {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value.as_str()? {
            "deposit" => Ok(TxType::Deposit),
            "withdrawal" => Ok(TxType::Withdrawal),
            "dispute" => Ok(TxType::Dispute),
            "resolve" => Ok(TxType::Resolve),
            "chargeback" => Ok(TxType::Chargeback),
            _ => Err(FromSqlError::from(FromSqlError::InvalidType)),
        }
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

impl FromSql for AccountStatus {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value.as_str()? {
            "active" => Ok(AccountStatus::Active),
            "blocked" => Ok(AccountStatus::Blocked),
            "inactive" => Ok(AccountStatus::Inactive),
            _ => Err(FromSqlError::from(FromSqlError::InvalidType)),
        }
    }
}

#[derive(SerdeSerialize)]
struct Account {
    pub client_id: ClientId,
    pub available: Amount,
    pub held: Amount,
    pub total: Amount,
    pub locked: bool,
}

impl Account {
    fn new(client_id: ClientId) -> Self {
        Account {
            client_id,
            available: 0f64,
            held: 0f64,
            total: 0f64,
            locked: false,
        }
    }
}

fn to_csv(accounts: Vec<Account>) -> Result<String> {
    let buf = Vec::new();
    let mut builder = csv::WriterBuilder::new().from_writer(buf);

    for acc in accounts {
        builder.serialize(acc)?;
    }

    let bytes = builder.into_inner().context("failed flushing into buffer or file")?;
    String::from_utf8(bytes).context("failed converting csv to string from byte vector")
}

fn from_sql_table(conn: &SqlConnection) -> Result<Vec<Account>> {
    let mut q = conn.prepare("SELECT id, available_amount, held_amount, locked, status from account;")
        .map_err(|err| anyhow::Error::from(err))?;

    let m = q.query_map([], |row| {
        let available = row.get(1)?;
        let held = row.get(2)?;
        let total = available + held;
        Ok(Account {
            client_id: row.get(0)?,
            available,
            held,
            total,
            locked: row.get(3)?,
        })
    }).map_err(|err| anyhow::Error::from(err))?;

    let a = m.map(|x| x.unwrap()).collect::<_>();

    Ok(a)
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

fn handle_deposit(conn: &mut SqlConnection, tx: &Tx) -> Result<()> {
    let mut dbtx = conn.transaction()?;

    let num_of_records: i64 = dbtx.query_row(
        "SELECT count(id) FROM tx where id = ?1",
        params![&tx.id],
        |row| Ok(row.get(0)?),
    )?;

    if num_of_records == 1 {
        return dbtx.rollback().context("failed rolling back transaction");
    }

    if let Err(e) = dbtx.execute(
        "INSERT OR IGNORE INTO account (id, available_amount, held_amount, locked, status) VALUES (?1, ?2, ?3, ?4, ?5);",
        params![tx.client_id, 0f64, 0f64, false, AccountStatus::Active])
    {
        dbtx.rollback().context("failed rolling back transaction")?;
        return Err(anyhow::Error::new(e));
    }


    if let Err(e) = dbtx.execute(
        "UPDATE account SET available_amount = available_amount + ?1 WHERE id = ?2 AND status = ?3;",
        params![tx.amount, tx.client_id, AccountStatus::Active])
    {
        dbtx.rollback().context("failed rolling back transaction")?;
        return Err(anyhow::Error::new(e));
    }

    if let Err(e) = dbtx.execute(
        "INSERT OR IGNORE INTO tx (id, tx_type, client_id, amount) values (?1, ?2, ?3, ?4);",
        params![tx.id, tx.tx_type, tx.client_id, tx.amount],
    ).map(|_| ()) {
        dbtx.rollback().context("failed rolling back transaction")?;
        return Err(anyhow::Error::new(e));
    }

    dbtx.commit().map(|_| ()).context("failed committing on deposit")
}

fn handle_withdrawal(conn: &mut SqlConnection, tx: &Tx) -> Result<()> {
    let mut dbtx = conn.transaction()?;

    let num_of_records: i64 = dbtx.query_row(
        "SELECT count(id) FROM tx where id = ?1",
        params![&tx.id],
        |row| Ok(row.get(0)?),
    )?;

    if num_of_records == 1 {
        dbtx.rollback().context("failed rolling back transaction")?;
        return Ok(());
    }

    dbtx.execute(
        "UPDATE account SET available_amount = available_amount - ?1 WHERE id = ?2 AND status = ?3 AND available_amount >= ?1;",
        params![tx.amount, tx.client_id, AccountStatus::Active])
        .context("failed updating account transaction on withdrawal")?;

    dbtx.execute(
        "INSERT OR IGNORE INTO tx (id, tx_type, client_id, amount) values (?1, ?2, ?3, ?4);",
        params![tx.id, tx.tx_type, tx.client_id, tx.amount],
    )
        .map(|x| ())
        .context("failed inserting processed transaction on withdrawal")?;

    dbtx.commit().map(|_| ()).context("failed committing on withdrawal")
}

fn handle_dispute(conn: &mut SqlConnection, tx: &Tx) -> Result<()> {
    let mut dbtx = conn.transaction()?;

    let txrecordres = dbtx.query_row(
        "SELECT id, tx_type, client_id, amount, status FROM tx WHERE status = ?3 AND client_id = ?1 AND id = ?2;",
        params![&tx.client_id, &tx.id, TxStatus::Processed.to_string()], |r| {
            let id: u32 = r.get(0)?;
            Ok(SqlTx {
                id,
                tx_type: r.get(1)?,
                client_id: r.get(2)?,
                amount: r.get(3)?,
                status: r.get(4)?,
            })
        });

    let txrecord = match txrecordres {
        Ok(txrecord) => txrecord,
        Err(e) => {
            if e == SqlError::QueryReturnedNoRows {
                return Ok(());
            }

            return Err(anyhow::Error::from(e));
        }
    };

    dbtx.execute(
        "UPDATE tx SET status = ?2 WHERE id = ?1;",
        params![&txrecord.id, TxStatus::InDispute],
    )
        .context("failed updating tx status on dispute")?;

    dbtx.execute(
        "UPDATE account SET available_amount = available_amount - ?1, held_amount = held_amount + ?1 WHERE id = ?2;",
        params![txrecord.amount, txrecord.client_id],
    )
        .map(|x| ())
        .context("failed updating account on dispute")?;

    dbtx.commit().map(|_| ()).context("failed committing on dispute")
}

fn handle_resolve(conn: &mut SqlConnection, tx: &Tx) -> Result<()> {
    let mut dbtx = conn.transaction()?;

    let txrecordres = dbtx.query_row(
        "SELECT id, tx_type, client_id, amount, status FROM tx WHERE status = ?3 AND client_id = ?1 AND id = ?2;",
        params![&tx.client_id, &tx.id, TxStatus::Processed.to_string()], |r| {
            let id: u32 = r.get(0)?;
            Ok(SqlTx {
                id,
                tx_type: r.get(1)?,
                client_id: r.get(2)?,
                amount: r.get(3)?,
                status: r.get(4)?,
            })
        });

    let txrecord = match txrecordres {
        Ok(txrecord) => txrecord,
        Err(e) => {
            if e == SqlError::QueryReturnedNoRows {
                return Ok(());
            }

            return Err(anyhow::Error::from(e));
        }
    };

    dbtx.execute(
        "UPDATE tx SET status = ?2 WHERE id = ?1;",
        params![&txrecord.id, TxStatus::Resolved],
    )
        .context("failed updating tx status on resolve");

    dbtx.execute(
        "UPDATE account SET available_amount = available_amount + ?1, held_amount = held_amount - ?1 WHERE id = ?2;",
        params![txrecord.amount, txrecord.client_id],
    )
        .map(|x| ())
        .context("failed updating account on resolve")?;

    dbtx.commit().map(|_| ()).context("failed committing resolve")
}

fn handle_chargeback(conn: &mut SqlConnection, tx: &Tx) -> Result<()> {
    let mut dbtx = conn.transaction()?;

    let txrecordres = dbtx.query_row(
        "SELECT id, tx_type, client_id, amount, status FROM tx WHERE status = ?3 AND client_id = ?1 AND id = ?2;",
        params![&tx.client_id, &tx.id, TxStatus::Processed.to_string()], |r| {
            let id: u32 = r.get(0)?;
            Ok(SqlTx {
                id,
                tx_type: r.get(1)?,
                client_id: r.get(2)?,
                amount: r.get(3)?,
                status: r.get(4)?,
            })
        });

    let txrecord = match txrecordres {
        Ok(txrecord) => txrecord,
        Err(e) => {
            if e == SqlError::QueryReturnedNoRows {
                return Ok(());
            }

            return Err(anyhow::Error::from(e));
        }
    };

    dbtx.execute(
        "UPDATE tx SET status = ?2 WHERE id = ?1;",
        params![&txrecord.id, TxStatus::Chargeback],
    )
        .context("failed updating transaction status on chargeback")?;

    dbtx.execute(
        "UPDATE account SET held_amount = held_amount - ?1, status = ?2 WHERE id = ?3;",
        params![txrecord.amount, AccountStatus::Blocked, txrecord.client_id],
    )
        .map(|x| ())
        .context("failed updating account on chargeback")?;

    dbtx.commit().map(|_| ()).context("failed committing chargeback")
}

fn handle_tx(conn: &mut SqlConnection, tx: Tx) -> Result<()> {
    match tx.tx_type {
        TxType::Deposit => handle_deposit(conn, &tx),
        TxType::Withdrawal => handle_withdrawal(conn, &tx),
        TxType::Dispute => handle_dispute(conn, &tx),
        TxType::Resolve => handle_resolve(conn, &tx),
        TxType::Chargeback => handle_chargeback(conn, &tx),
    }
}

fn main() -> Result<()> {
    let mut conn = SqlConnection::open("test.db")?;

    let args: Vec<String> = std::env::args().collect();

    if args.len() > 2 {
        return Err(anyhow::anyhow!("expected 1 argument, got {}. Try cargo run -- transactions.csv > accounts.csv", args.len()));
    }

    let input_path = &args[1];

    let txs = read_csv(PathBuf::from(input_path)).unwrap();
    let mut queue = TxQueue::new();

    conn.execute("CREATE TABLE IF NOT EXISTS tx (id INTEGER PRIMARY KEY, tx_type TEXT, client_id INTEGER, amount DOUBLE PRECISION, status TEXT DEFAULT 'processed');", []).unwrap();
    conn.execute("CREATE TABLE IF NOT EXISTS account (id INTEGER PRIMARY KEY, available_amount DOUBLE PRECISION , held_amount DOUBLE PRECISION, locked BOOLEAN, status TEXT DEFAULT 'active');", []).unwrap();

    for tx in txs {
        queue.push(tx);
    }

    while let Some(tx) = queue.pop() {
        handle_tx(&mut conn, tx);
    }

    println!("{}", to_csv(from_sql_table(&conn)?)?);
    Ok(())
}

#[cfg(test)]
mod component_tests {
    #[test]
    fn should_succeed_on_processing_tx_variant_1() {}

    fn should_succeed_on_processing_tx_variant_2() {}

    fn should_succeed_on_processing_tx_variant_3() {}

    fn should_succeed_on_processing_tx_variant_4() {}

    fn should_succeed_on_processing_tx_variant_5() {}
}
