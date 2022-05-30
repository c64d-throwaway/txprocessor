use anyhow::{anyhow, Context, Result};
use rusqlite::{
    params,
    types::{FromSql, FromSqlError, FromSqlResult, ToSqlOutput, ValueRef},
    Connection as SqlConnection, Error as SqlError, Result as SqlResult, ToSql,
    Transaction as SqlTransaction,
};
use serde::{de, Deserialize, Deserializer};
use serde_derive::{Deserialize as SerdeDeserialize, Serialize as SerdeSerialize};
use std::{collections::VecDeque, fs::OpenOptions};
use strum_macros::{Display, EnumString};

type ClientId = u16;
type TxId = u32;
type Amount = f64;

/// SQL
#[derive(Debug, SerdeDeserialize)]
struct SqlTx {
    pub id: TxId,
    pub tx_type: TxType,
    pub client_id: ClientId,
    pub amount: Amount,
    pub status: TxStatus,
}

fn from_sql_table(conn: &SqlConnection) -> Result<Vec<Account>> {
    let mut q = conn
        .prepare("SELECT id, available_amount, held_amount, locked, status from account;")
        .map_err(anyhow::Error::from)?;

    let m = q
        .query_map([], |row| {
            let available = row.get(1)?;
            let held = row.get(2)?;
            let total = available + held;
            let status: String = row.get(4)?;
            let locked = status == AccountStatus::Blocked.to_string();

            Ok(Account {
                client_id: row.get(0)?,
                available,
                held,
                total,
                locked,
            })
        })
        .map_err(anyhow::Error::from)?;

    m.map(|x| x.map_err(anyhow::Error::from))
        .collect::<Result<Vec<Account>>>()
}

fn migrate_tables(conn: &mut SqlConnection) -> Result<()> {
    let dbtx = conn.transaction()?;
    dbtx.execute("CREATE TABLE IF NOT EXISTS tx (id INTEGER PRIMARY KEY, tx_type TEXT, client_id INTEGER, amount DOUBLE PRECISION, status TEXT DEFAULT 'processed');", [])
        .context("failed migrating tx table")?;

    dbtx.execute("CREATE TABLE IF NOT EXISTS account (id INTEGER PRIMARY KEY, available_amount DOUBLE PRECISION , held_amount DOUBLE PRECISION, locked BOOLEAN, status TEXT DEFAULT 'active');", [])
        .context("failed migrating account table").map(|_| ())?;

    dbtx.commit()
        .map(|_| ())
        .context("failed committing migrations")
}

/// CSV
fn to_csv(accounts: Vec<Account>) -> Result<String> {
    let buf = Vec::new();
    let mut builder = csv::WriterBuilder::new().from_writer(buf);

    for acc in accounts {
        builder.serialize(acc)?;
    }

    let bytes = builder
        .into_inner()
        .context("failed flushing into buffer or file")?;
    String::from_utf8(bytes).context("failed converting csv to string from byte vector")
}

/// General domain types and functions
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

#[derive(Debug, EnumString, Display)]
enum TxType {
    #[strum(serialize = "deposit")]
    Deposit,
    #[strum(serialize = "withdrawal")]
    Withdrawal,
    #[strum(serialize = "dispute")]
    Dispute,
    #[strum(serialize = "resolve")]
    Resolve,
    #[strum(serialize = "chargeback")]
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
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

#[derive(Debug, EnumString, Display)]
enum TxStatus {
    #[strum(serialize = "processed")]
    Processed,
    #[strum(serialize = "in_dispute")]
    InDispute,
    #[strum(serialize = "resolved")]
    Resolved,
    #[strum(serialize = "chargeback")]
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
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

#[derive(Debug, EnumString, Display)]
enum AccountStatus {
    #[strum(serialize = "active")]
    Active,
    #[strum(serialize = "blocked")]
    Blocked,
    #[strum(serialize = "inactive")]
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
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

#[derive(Debug, PartialEq, SerdeSerialize)]
struct Account {
    pub client_id: ClientId,
    pub available: Amount,
    pub held: Amount,
    pub total: Amount,
    pub locked: bool,
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
}

fn handle_deposit(conn: &mut SqlConnection, tx: &Tx) -> Result<()> {
    let dbtx = conn.transaction()?;

    let num_of_records: i64 = dbtx.query_row(
        "SELECT count(id) FROM tx where id = ?1",
        params![&tx.id],
        |row| row.get(0),
    )?;

    if num_of_records == 1 {
        return dbtx.rollback().context("failed rolling back transaction");
    }

    dbtx.execute(
        "INSERT OR IGNORE INTO account (id, available_amount, held_amount, locked, status) VALUES (?1, ?2, ?3, ?4, ?5);",
        params![tx.client_id, 0f64, 0f64, false, AccountStatus::Active])?;

    dbtx.execute(
        "UPDATE account SET available_amount = available_amount + ?1 WHERE id = ?2 AND status = ?3;",
        params![tx.amount, tx.client_id, AccountStatus::Active])?;

    dbtx.execute(
        "INSERT OR IGNORE INTO tx (id, tx_type, client_id, amount) values (?1, ?2, ?3, ?4);",
        params![tx.id, tx.tx_type, tx.client_id, tx.amount],
    )?;

    dbtx.commit()
        .map(|_| ())
        .context("failed committing on deposit")
}

fn handle_withdrawal(conn: &mut SqlConnection, tx: &Tx) -> Result<()> {
    let dbtx = conn.transaction()?;

    let num_of_records: i64 = dbtx.query_row(
        "SELECT count(id) FROM tx where id = ?1",
        params![&tx.id],
        |row| row.get(0),
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
    .map(|_| ())
    .context("failed inserting processed transaction on withdrawal")?;

    dbtx.commit()
        .map(|_| ())
        .context("failed committing on withdrawal")
}

fn handle_missing_tx(dbtx: &SqlTransaction, tx: &Tx, tx_status: TxStatus) -> SqlResult<SqlTx> {
    dbtx.query_row(
        "SELECT id, tx_type, client_id, amount, status FROM tx WHERE status = ?3 AND client_id = ?1 AND id = ?2;",
        params![&tx.client_id, &tx.id, tx_status], |r| {
            let id: u32 = r.get(0)?;
            Ok(SqlTx {
                id,
                tx_type: r.get(1)?,
                client_id: r.get(2)?,
                amount: r.get(3)?,
                status: r.get(4)?,
            })
        })
}

fn handle_dispute(conn: &mut SqlConnection, tx: &Tx) -> Result<()> {
    let dbtx = conn.transaction()?;
    let txrecord = match handle_missing_tx(&dbtx, tx, TxStatus::Processed) {
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
        .map(|_| ())
        .context("failed updating account on dispute")?;

    dbtx.commit()
        .map(|_| ())
        .context("failed committing on dispute")
}

fn handle_resolve(conn: &mut SqlConnection, tx: &Tx) -> Result<()> {
    let dbtx = conn.transaction()?;
    let txrecord = match handle_missing_tx(&dbtx, tx, TxStatus::InDispute) {
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
    .context("failed updating tx status on resolve")?;

    dbtx.execute(
        "UPDATE account SET available_amount = available_amount + ?1, held_amount = held_amount - ?1 WHERE id = ?2;",
        params![txrecord.amount, txrecord.client_id],
    )
        .map(|_| ())
        .context("failed updating account on resolve")?;

    dbtx.commit()
        .map(|_| ())
        .context("failed committing resolve")
}

fn handle_chargeback(conn: &mut SqlConnection, tx: &Tx) -> Result<()> {
    let dbtx = conn.transaction()?;
    let txrecord = match handle_missing_tx(&dbtx, tx, TxStatus::InDispute) {
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
    .map(|_| ())
    .context("failed updating account on chargeback")?;

    dbtx.commit()
        .map(|_| ())
        .context("failed committing chargeback")?;

    Ok(())
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

// CLI app related types and functions
fn source_file_from_args() -> Result<String> {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        return Err(anyhow::anyhow!(
            "too few arguments: cargo run -- <input_file>.csv > <output_file>.csv"
        ));
    }

    Ok(args[1].clone())
}

fn main() -> Result<()> {
    // setup database and connections
    let mut conn = SqlConnection::open("test.db")?;
    migrate_tables(&mut conn)?;
    let mut queue = TxQueue::new();
    // get cli args
    let input_path = source_file_from_args()?;

    // read from CSV
    let txfile = OpenOptions::new().read(true).open(&input_path)?;
    let mut rdr = csv::Reader::from_reader(txfile);
    let mut raw_record = csv::StringRecord::new();
    let headers = rdr.headers()?.clone();

    while rdr.read_record(&mut raw_record)? {
        let tx: Tx = raw_record.deserialize(Some(&headers))?;
        queue.push(tx);
    }

    // read from queue
    while let Some(tx) = queue.pop() {
        handle_tx(&mut conn, tx)?;
    }

    // out
    print!("{}", to_csv(from_sql_table(&conn)?)?);

    if let Err(e) = conn.close() {
        return Err(anyhow!("failed closing database connection {}", e.1));
    }

    Ok(())
}

#[cfg(test)]
mod component_tests {
    use crate::{from_sql_table, handle_tx, migrate_tables, Account, Tx, TxQueue};
    use anyhow::{Context, Result};
    use rusqlite::Connection as SqlConnection;
    use std::io::Read;

    fn setup() -> Result<SqlConnection> {
        let mut conn = SqlConnection::open_in_memory()?;
        migrate_tables(&mut conn)?;

        return Ok(conn);
    }

    fn run(conn: &mut SqlConnection, csv: &str) -> Result<()> {
        let buf = std::io::BufReader::new(csv.as_bytes());
        let txs = read_csv(buf)?;

        let mut queue = TxQueue::new();

        for tx in txs {
            queue.push(tx);
        }

        while let Some(tx) = queue.pop() {
            handle_tx(conn, tx)?;
        }

        Ok(())
    }

    fn read_csv(rdr: impl Read) -> Result<Vec<Tx>> {
        let mut b = csv::ReaderBuilder::new().from_reader(rdr);
        b.deserialize()
            .map(|x| {
                let tx: Tx = x.context("failed deserializing csv record into a transaction")?;
                Ok(tx)
            })
            .collect::<Result<_>>()
    }

    #[test]
    #[should_panic]
    fn should_fail_on_invalid_tx_type() {
        let mut conn = setup().unwrap();
        let csv = r#"type,client,tx,amount
wrong_type,1,1,1.0"#;

        run(&mut conn, csv).unwrap();
    }

    #[test]
    fn should_succeed_on_processing_tx_variant_1() {
        let mut conn = setup().unwrap();
        let csv = r#"type,client,tx,amount
deposit,1,1,1.0
deposit,2,2,2.0
deposit,1,3,2.0
withdrawal,1,1,0.0
withdrawal,2,2,0.0"#;
        let expected_result = vec![
            Account {
                client_id: 1,
                available: 3.0,
                held: 0.0,
                total: 3.0,
                locked: false,
            },
            Account {
                client_id: 2,
                available: 2.0,
                held: 0.0,
                total: 2.0,
                locked: false,
            },
        ];
        run(&mut conn, csv).unwrap();
        assert_eq!(from_sql_table(&conn).unwrap(), expected_result);
    }

    #[test]
    fn should_succeed_on_processing_tx_variant_2() {
        let mut conn = setup().unwrap();
        let csv = r#"type,client,tx,amount
deposit,1,1,1.0
deposit,2,2,2.0
deposit,1,3,2.0
dispute,1,1,
dispute,2,2,
resolve,2,2,
chargeback,1,1,"#;
        let expected_result = vec![
            Account {
                client_id: 1,
                available: 2.0,
                held: 0.0,
                total: 2.0,
                locked: true,
            },
            Account {
                client_id: 2,
                available: 2.0,
                held: 0.0,
                total: 2.0,
                locked: false,
            },
        ];

        run(&mut conn, csv).unwrap();
        assert_eq!(from_sql_table(&conn).unwrap(), expected_result);
    }

    #[test]
    fn should_succeed_on_processing_tx_variant_3() {
        let mut conn = setup().unwrap();
        let csv = r#"type,client,tx,amount
deposit,1,1,1.0
deposit,2,2,2.0
deposit,1,3,2.0
dispute,1,1,
dispute,2,2,
resolve,2,2,
resolve,2,2,
dispute,1,1,
chargeback,1,1,
chargeback,1,1,
deposit,1,1,1.0"#;
        let expected_result = vec![
            Account {
                client_id: 1,
                available: 2.0,
                held: 0.0,
                total: 2.0,
                locked: true,
            },
            Account {
                client_id: 2,
                available: 2.0,
                held: 0.0,
                total: 2.0,
                locked: false,
            },
        ];

        run(&mut conn, csv).unwrap();
        assert_eq!(from_sql_table(&conn).unwrap(), expected_result);
    }

    #[test]
    fn should_succeed_on_processing_tx_variant_4() {
        let mut conn = setup().unwrap();
        let csv = r#"type,client,tx,amount
deposit,1,1,1.0
deposit,2,2,2.0
deposit,1,3,2.0
deposit,3,4,2.0
dispute,1,1,
dispute,2,2,
resolve,2,2,
resolve,2,2,
dispute,1,1,
withdrawal,3,5,2.0
chargeback,1,1,
chargeback,1,1,
deposit,1,1,1.0"#;
        let expected_result = vec![
            Account {
                client_id: 1,
                available: 2.0,
                held: 0.0,
                total: 2.0,
                locked: true,
            },
            Account {
                client_id: 2,
                available: 2.0,
                held: 0.0,
                total: 2.0,
                locked: false,
            },
            Account {
                client_id: 3,
                available: 0.0,
                held: 0.0,
                total: 0.0,
                locked: false,
            },
        ];

        run(&mut conn, csv).unwrap();
        assert_eq!(from_sql_table(&conn).unwrap(), expected_result);
    }
}
