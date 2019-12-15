use crate::scheduler::schedproto::{WorkerId, WorkerInfo, TaskId};
use std::collections::HashSet;

pub struct Worker {
    pub id: WorkerId,
    pub ncpus: u32,
    pub load_ncpus: u32,
    pub tasks: HashSet<TaskId>,
}

pub type WorkerRef = crate::common::WrappedRcRefCell<Worker>;

impl WorkerRef {
    pub fn new(wi: WorkerInfo) -> Self {
        Self::wrap(Worker {
            id: wi.id,
            ncpus: wi.n_cpus,
            load_ncpus: 0,
            tasks: Default::default(),
        })
    }
}