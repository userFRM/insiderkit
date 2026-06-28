//! The insider-transaction record and its role enum.
//!
//! One [`Txn`] is one reported Form 3/4/5 transaction line. Dates are stored as
//! `i32` in `YYYYMMDD` form (e.g. `20240401`) so comparisons are integer-cheap
//! and need no calendar library on the hot path.
use serde::{Deserialize, Serialize};

/// The reporting owner's relationship to the issuer, collapsed to one role.
///
/// SEC stores the relationship as comma-joined text (e.g. `"Director,Officer"`);
/// [`Role::from_relationship`] picks the most senior single role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    Director,
    Officer,
    TenPct,
    Other,
}

impl Role {
    /// Collapse SEC's comma-joined relationship text to a single role.
    ///
    /// Precedence: Director, then Officer, then TenPercentOwner, then Other.
    pub fn from_relationship(text: &str) -> Role {
        let t = text.to_ascii_lowercase();
        if t.contains("director") {
            Role::Director
        } else if t.contains("officer") {
            Role::Officer
        } else if t.contains("tenpercent") || t.contains("ten percent") {
            Role::TenPct
        } else {
            Role::Other
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Role::Director => "director",
            Role::Officer => "officer",
            Role::TenPct => "tenpct",
            Role::Other => "other",
        }
    }

    pub fn parse(s: &str) -> Option<Role> {
        match s {
            "director" => Some(Role::Director),
            "officer" => Some(Role::Officer),
            "tenpct" => Some(Role::TenPct),
            "other" => Some(Role::Other),
            _ => None,
        }
    }
}

/// One reported insider transaction (one row in the bundled parquet).
///
/// Dates are `i32` `YYYYMMDD`. `ticker` and `officer_title` may be empty when
/// the filing omits them.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Txn {
    pub filing_date: i32,
    pub accession: String,
    pub doc_type: String,
    pub issuer_cik: u32,
    pub issuer_name: String,
    pub ticker: String,
    pub owner_cik: u32,
    pub owner_name: String,
    pub role: Role,
    pub officer_title: String,
    pub security_title: String,
    pub txn_date: i32,
    pub txn_code: String,
    pub shares: f64,
    pub price: f64,
    pub acquired_disposed: String,
    pub shares_owned_after: f64,
    pub direct_indirect: String,
    pub is_derivative: bool,
}

impl Txn {
    /// `true` for an open-market or private purchase (transaction code `P`).
    pub fn is_buy(&self) -> bool {
        self.txn_code == "P"
    }

    /// `true` for an open-market or private sale (transaction code `S`).
    pub fn is_sell(&self) -> bool {
        self.txn_code == "S"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn role_precedence() {
        assert_eq!(Role::from_relationship("Director,Officer"), Role::Director);
        assert_eq!(
            Role::from_relationship("Officer,TenPercentOwner"),
            Role::Officer
        );
        assert_eq!(Role::from_relationship("TenPercentOwner"), Role::TenPct);
        assert_eq!(Role::from_relationship("Other"), Role::Other);
        assert_eq!(Role::from_relationship(""), Role::Other);
    }

    #[test]
    fn role_round_trip() {
        for r in [Role::Director, Role::Officer, Role::TenPct, Role::Other] {
            assert_eq!(Role::parse(r.as_str()), Some(r));
        }
    }
}
