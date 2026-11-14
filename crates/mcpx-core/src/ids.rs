use crate::{Error, Result};

/// Largest opaque ID representable without rounding in every supported host.
/// WIT carries u64, while the JSON/JavaScript bridge uses exact Number integers.
pub const MAX_HOST_ID: u64 = (1 << 53) - 1;
const STRIDE: u64 = 16;

/// Owner-local domains never share an encoded ID. Tags are an internal detail;
/// hosts use the explicitly named action/RPC/operation/resource fields.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IdKind {
    Action = 1,
    Rpc = 2,
    Operation = 3,
    Binding = 4,
    Continuation = 5,
    Notification = 6,
}
impl IdKind {
    pub(crate) const fn first(self) -> u64 {
        self as u64
    }
    pub(crate) const fn last(self) -> u64 {
        MAX_HOST_ID - (MAX_HOST_ID - self.first()) % STRIDE
    }
    pub(crate) fn contains(self, id: u64) -> bool {
        (self.first()..=self.last()).contains(&id) && id % STRIDE == self.first()
    }
    pub(crate) fn available(self, next: u64) -> Result<()> {
        if !self.contains(next) {
            return Err(Error::local("LimitExceeded", "remote ID domain exhausted"));
        }
        Ok(())
    }
    pub(crate) fn allocate(self, next: &mut u64) -> Result<u64> {
        self.available(*next)?;
        let id = *next;
        *next += STRIDE;
        Ok(id)
    }
    pub(crate) fn increment(self, previous: &mut u64) -> Result<()> {
        let mut next = if *previous == 0 {
            self.first()
        } else {
            previous.saturating_add(STRIDE)
        };
        *previous = self.allocate(&mut next)?;
        Ok(())
    }
}

pub(crate) fn available(next: u64) -> Result<()> {
    if !(1..=MAX_HOST_ID).contains(&next) {
        return Err(Error::local("LimitExceeded", "remote ID space exhausted"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn domains_remain_disjoint_above_u32_and_stop_before_rounding() {
        let kinds = [
            IdKind::Action,
            IdKind::Rpc,
            IdKind::Operation,
            IdKind::Binding,
            IdKind::Continuation,
            IdKind::Notification,
        ];
        for kind in kinds {
            let mut next = (1 << 32) + kind.first();
            let id = kind.allocate(&mut next).unwrap();
            assert_eq!(id, (1 << 32) + kind.first());
            for other in kinds {
                assert_eq!(other.contains(id), other == kind);
            }
            next = kind.last();
            assert_eq!(kind.allocate(&mut next).unwrap(), kind.last());
            assert_eq!(kind.allocate(&mut next).unwrap_err().code, "LimitExceeded");
            assert!(next > MAX_HOST_ID);
            assert_eq!(kind.allocate(&mut 0).unwrap_err().code, "LimitExceeded");
            let mut exhausted = u64::MAX;
            assert_eq!(
                kind.increment(&mut exhausted).unwrap_err().code,
                "LimitExceeded"
            );
        }
    }
}
