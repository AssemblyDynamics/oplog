//! The operation module is responsible for converting MongoDB BSON documents into specific
//! `Operation` types, one for each type of document stored in the MongoDB oplog. As much as
//! possible, we convert BSON types into more typical Rust types (e.g. BSON timestamps into UTC
//! datetimes).
//!
//! As we accept _any_ document, it may not be a valid operation so wrap any conversions in a
//! `Result`.

use std::fmt;

use crate::{Error, Result};
use base64::encode;
use bson::{Bson, Document};
use chrono::{DateTime, TimeZone, Utc};
use mongodb::bson;

/// A MongoDB oplog operation.
#[derive(Clone, Debug, PartialEq)]
pub enum Operation {
    /// A no-op as inserted periodically by MongoDB or used to initiate new replica sets.
    Noop {
        /// A unique identifier for this operation.
        uid: String,
        /// The time of the operation.
        timestamp: DateTime<Utc>,
        /// The message associated with this operation.
        message: Option<String>,
    },
    /// An insert of a document into a specific database and collection.
    Insert {
        /// A unique identifier for this operation.
        uid: String,
        /// The time of the operation.
        timestamp: DateTime<Utc>,
        /// The full namespace of the operation including its database and collection.
        namespace: String,
        /// The BSON document inserted into the namespace.
        document: Document,
    },
    /// An update of a document in a specific database and collection matching a given query.
    Update {
        /// A unique identifier for this operation.
        uid: String,
        /// The time of the operation.
        timestamp: DateTime<Utc>,
        /// The full namespace of the operation including its database and collection.
        namespace: String,
        /// The BSON selection criteria for the update.
        query: Document,
        /// The BSON update applied in this operation.
        update: Document,
    },
    /// The deletion of a document in a specific database and collection matching a given query.
    Delete {
        /// A unique identifier for this operation.
        uid: String,
        /// The time of the operation.
        timestamp: DateTime<Utc>,
        /// The full namespace of the operation including its database and collection.
        namespace: String,
        /// The BSON selection criteria for the delete.
        query: Document,
    },
    /// A command such as the creation or deletion of a collection.
    Command {
        /// A unique identifier for this operation.
        uid: String,
        /// The time of the operation.
        timestamp: DateTime<Utc>,
        /// The full namespace of the operation including its database and collection.
        namespace: String,
        /// The BSON command.
        command: Document,
    },
    /// A command to apply multiple oplog operations at once.
    ApplyOps {
        /// A unique identifier for this operation.
        uid: String,
        /// The time of the operation.
        timestamp: DateTime<Utc>,
        /// The full namespace of the operation including its database and collection.
        namespace: String,
        /// A vector of operations to apply.
        operations: Vec<Operation>,
    },
}

impl Operation {
    /// Try to create a new Operation from a BSON document.
    ///
    /// # Example
    ///
    /// ```
    /// # #[macro_use]
    /// # use oplog::bson::{self, Bson, doc};
    /// use oplog::Operation;
    ///
    /// # fn main() {
    /// let document = doc! {
    ///     "ts": Bson::Timestamp(bson::Timestamp {
    ///         time: 1479561394,
    ///         increment: 0,
    ///     }),
    ///     "h": (-1742072865587022793i64),
    ///     "v": 2,
    ///     "op": "i",
    ///     "ns": "foo.bar",
    ///     "o": {
    ///         "foo": "bar"
    ///     }
    /// };
    /// let operation = Operation::new(&document);
    /// # }
    /// ```
    pub fn new(document: &Document) -> Result<Operation> {
        let op = document.get_str("op")?;

        match op {
            "n" => Operation::from_noop(document),
            "i" => Operation::from_insert(document),
            "u" => Operation::from_update(document),
            "d" => Operation::from_delete(document),
            "c" => Operation::from_command(document),
            op => Err(Error::UnknownOperation(op.into())),
        }
    }

    /// Returns an operation from any BSON value.
    fn from_bson(bson: &Bson) -> Result<Operation> {
        match *bson {
            Bson::Document(ref document) => Operation::new(document),
            _ => Err(Error::InvalidOperation),
        }
    }

    /// Returns a no-op operation for a given document.
    fn from_noop(document: &Document) -> Result<Operation> {
        let ts = document.get_timestamp("ts")?;
        // We don't always get a document in "o"
        let message = document
            .get("o")
            .and_then(|d| d.as_document())
            .and_then(|d| d.get("msg"))
            .and_then(|d| d.as_str())
            .map(|s| s.to_string());

        Ok(Operation::Noop {
            uid: get_uid(document).unwrap().to_string(),
            timestamp: timestamp_to_datetime(ts),
            message,
        })
    }

    /// Return an insert operation for a given document.
    fn from_insert(document: &Document) -> Result<Operation> {
        let ts = document.get_timestamp("ts")?;
        let ns = document.get_str("ns")?;
        let o = document.get_document("o")?;

        Ok(Operation::Insert {
            uid: get_uid(document).unwrap().to_string(),
            timestamp: timestamp_to_datetime(ts),
            namespace: ns.into(),
            document: o.to_owned(),
        })
    }

    /// Return an update operation for a given document.
    fn from_update(document: &Document) -> Result<Operation> {
        let ts = document.get_timestamp("ts")?;
        let ns = document.get_str("ns")?;
        let o = document.get_document("o")?;
        let o2 = document.get_document("o2")?;

        Ok(Operation::Update {
            uid: get_uid(document).unwrap().to_string(),
            timestamp: timestamp_to_datetime(ts),
            namespace: ns.into(),
            query: o2.to_owned(),
            update: o.to_owned(),
        })
    }

    /// Return a delete operation for a given document.
    fn from_delete(document: &Document) -> Result<Operation> {
        let ts = document.get_timestamp("ts")?;
        let ns = document.get_str("ns")?;
        let o = document.get_document("o")?;

        Ok(Operation::Delete {
            uid: get_uid(document).unwrap().to_string(),
            timestamp: timestamp_to_datetime(ts),
            namespace: ns.into(),
            query: o.to_owned(),
        })
    }

    /// Return a command operation for a given document.
    ///
    /// Note that this can return either an `Operation::Command` or an `Operation::ApplyOps` when
    /// successful.
    fn from_command(document: &Document) -> Result<Operation> {
        let ts = document.get_timestamp("ts")?;
        let ns = document.get_str("ns")?;
        let o = document.get_document("o")?;

        match o.get_array("applyOps") {
            Ok(ops) => {
                let operations = ops
                    .iter()
                    .map(|bson| Operation::from_bson(bson))
                    .collect::<Result<Vec<Operation>>>()?;

                Ok(Operation::ApplyOps {
                    uid: get_uid(document).unwrap().to_string(),
                    timestamp: timestamp_to_datetime(ts),
                    namespace: ns.into(),
                    operations: operations,
                })
            }
            Err(_) => Ok(Operation::Command {
                uid: get_uid(document).unwrap().to_string(),
                timestamp: timestamp_to_datetime(ts),
                namespace: ns.into(),
                command: o.to_owned(),
            }),
        }
    }
}

impl fmt::Display for Operation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Operation::Noop {
                ref uid,
                timestamp,
                ref message,
            } => {
                write!(f, "No-op #{} at {}: {:?}", uid, timestamp, message)
            }
            Operation::Insert {
                ref uid,
                timestamp,
                ref namespace,
                ref document,
            } => {
                write!(
                    f,
                    "Insert #{} into {} at {}: {}",
                    uid, namespace, timestamp, document
                )
            }
            Operation::Update {
                ref uid,
                timestamp,
                ref namespace,
                ref query,
                ref update,
            } => {
                write!(
                    f,
                    "Update #{} {} with {} at {}: {}",
                    uid, namespace, query, timestamp, update
                )
            }
            Operation::Delete {
                ref uid,
                timestamp,
                ref namespace,
                ref query,
            } => {
                write!(
                    f,
                    "Delete #{} from {} at {}: {}",
                    uid, namespace, timestamp, query
                )
            }
            Operation::Command {
                ref uid,
                timestamp,
                ref namespace,
                ref command,
            } => {
                write!(
                    f,
                    "Command #{} {} at {}: {}",
                    uid, namespace, timestamp, command
                )
            }
            Operation::ApplyOps {
                ref uid,
                timestamp,
                ref namespace,
                ref operations,
            } => {
                write!(
                    f,
                    "ApplyOps #{} {} at {}: {} operations",
                    uid,
                    namespace,
                    timestamp,
                    operations.len()
                )
            }
        }
    }
}

/// Convert a BSON timestamp into a UTC `DateTime`.
fn timestamp_to_datetime(timestamp: bson::Timestamp) -> DateTime<Utc> {
    let seconds = timestamp.time;
    let nanoseconds = timestamp.increment;

    Utc.timestamp(seconds as i64, nanoseconds)
}

fn get_uid(document: &Document) -> Result<String> {
    let lsid = document.get_document("lsid")?;
    let bytes = lsid.get_binary_generic("uid")?;

    let uid = encode(bytes);

    Ok(uid)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    #[test]
    fn operation_converts_noops() {
        let doc = doc! {
            "ts" : Bson::Timestamp(bson::Timestamp {
                time: 1479419535 ,
                increment: 0,
            }),
            "h" : (-2135725856567446411i64),
            "v" : 2,
            "op" : "n",
            "ns" : "",
            "o" : {
                "msg" : "initiating set"
            }
        };
        let operation = Operation::new(&doc).unwrap();

        assert_eq!(
            operation,
            Operation::Noop {
                uid: "2135725856567446411i64".to_string(),
                timestamp: Utc.timestamp(1479419535, 0),
                message: Some("initiating set".into()),
            }
        );
    }

    #[test]
    fn operation_converts_inserts() {
        let doc = doc! {
            "ts" : Bson::Timestamp(bson::Timestamp {
                time: 1479561394 ,
                increment:0
            }),
            "h" : (-1742072865587022793i64),
            "v" : 2,
            "op" : "i",
            "ns" : "foo.bar",
            "o" : {
                "foo" : "bar"
            }
        };
        let operation = Operation::new(&doc).unwrap();

        assert_eq!(
            operation,
            Operation::Insert {
                uid: "1742072865587022793i64".to_string(),
                timestamp: Utc.timestamp(1479561394, 0),
                namespace: "foo.bar".into(),
                document: doc! { "foo" : "bar" },
            }
        );
    }

    #[test]
    fn operation_converts_updates() {
        let doc = doc! {
            "ts" : Bson::Timestamp(bson::Timestamp {
                time: 1479561033 ,
                increment: 0,
            }),
            "h" : (3511341713062188019i64),
            "v" : 2,
            "op" : "u",
            "ns" : "foo.bar",
            "o2" : {
                "_id" : 1
            },
            "o" : {
                "$set" : {
                    "foo" : "baz"
                }
            }
        };
        let operation = Operation::new(&doc).unwrap();

        assert_eq!(
            operation,
            Operation::Update {
                uid: "3511341713062188019i64".to_string(),
                timestamp: Utc.timestamp(1479561033, 0),
                namespace: "foo.bar".into(),
                query: doc! { "_id" : 1 },
                update: doc! { "$set" : { "foo" : "baz" } },
            }
        );
    }

    #[test]
    fn operation_converts_deletes() {
        let doc = doc! {
            "ts" : Bson::Timestamp(bson::Timestamp {
                time: 1479421186 ,
                increment: 0,
            }),
            "h" : (-5457382347563537847i64),
            "v" : 2,
            "op" : "d",
            "ns" : "foo.bar",
            "o" : {
                "_id" : 1
            }
        };
        let operation = Operation::new(&doc).unwrap();

        assert_eq!(
            operation,
            Operation::Delete {
                uid: "5457382347563537847i64".to_string(),
                timestamp: Utc.timestamp(1479421186, 0),
                namespace: "foo.bar".into(),
                query: doc! { "_id" : 1 },
            }
        );
    }

    #[test]
    fn operation_converts_commands() {
        let doc = doc! {
            "ts" : Bson::Timestamp(bson::Timestamp {
                time: 1479553955 ,
                increment: 0,
            }),
            "h" : (-7222343681970774929i64),
            "v" : 2,
            "op" : "c",
            "ns" : "test.$cmd",
            "o" : {
                "create" : "foo"
            }
        };
        let operation = Operation::new(&doc).unwrap();

        assert_eq!(
            operation,
            Operation::Command {
                uid: "7222343681970774929i6".to_string(),
                timestamp: Utc.timestamp(1479553955, 0),
                namespace: "test.$cmd".into(),
                command: doc! { "create" : "foo" },
            }
        );
    }

    #[test]
    fn operation_returns_unknown_operations() {
        let doc = doc! { "op" : "x" };
        let operation = Operation::new(&doc);

        match operation {
            Err(Error::UnknownOperation(op)) => assert_eq!(op, "x"),
            _ => panic!("Expected unknown operation."),
        }
    }

    #[test]
    fn operation_returns_missing_fields() {
        use bson::document::ValueAccessError;

        let doc = doc! { "foo" : "bar" };
        let operation = Operation::new(&doc);

        match operation {
            Err(Error::MissingField(err)) => assert_eq!(err, ValueAccessError::NotPresent),
            _ => panic!("Expected missing field."),
        }
    }

    #[test]
    fn operation_returns_apply_ops() {
        let doc = doc! {
            "ts" : Bson::Timestamp(bson::Timestamp {
                time: 1483789052 ,
                increment: 0,
            }),
            "h" : (-3262249347345468996i64),
            "v" : 2,
            "op" : "c",
            "ns" : "foo.$cmd",
            "o" : {
                "applyOps" : [
                    {
                        "ts" : Bson::Timestamp(bson::Timestamp {
                            time: 1479561394 ,
                            increment: 0,
                        }),
                        "t" : 2,
                        "h" : (-1742072865587022793i64),
                        "op" : "i",
                        "ns" : "foo.bar",
                        "o" : {
                            "_id" : 1,
                            "foo" : "bar"
                        }
                    }
                ]
            }
        };
        let operation = Operation::new(&doc).unwrap();

        assert_eq!(
            operation,
            Operation::ApplyOps {
                uid: "3262249347345468996i64".to_string(),
                timestamp: Utc.timestamp(1483789052, 0),
                namespace: "foo.$cmd".into(),
                operations: vec![Operation::Insert {
                    uid: "1742072865587022793i64".to_string(),
                    timestamp: Utc.timestamp(1479561394, 0),
                    namespace: "foo.bar".into(),
                    document: doc! { "_id" : 1, "foo" : "bar" },
                }],
            }
        );
    }
}
