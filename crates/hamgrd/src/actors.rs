//! Actors
//!
//! <https://github.com/r12f/SONiC/blob/user/r12f/hamgrd/doc/smart-switch/high-availability/smart-switch-ha-hamgrd.md#2-key-actors>

pub mod dpu;
pub mod vdpu;

pub mod ha_scope;
pub mod ha_set;

#[cfg(test)]
pub mod test;
