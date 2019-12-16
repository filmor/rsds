use crate::scheduler::schedproto::{WorkerId, WorkerInfo, TaskId};
use std::collections::HashSet;
use crate::scheduler::implementation::task::TaskRef;

pub struct Worker {
    pub id: WorkerId,
    pub ncpus: u32,
    pub tasks: HashSet<TaskRef>,
}

pub type WorkerRef = crate::common::WrappedRcRefCell<Worker>;

impl WorkerRef {
    pub fn new(wi: WorkerInfo) -> Self {
        Self::wrap(Worker {
            id: wi.id,
            ncpus: wi.n_cpus,
            tasks: Default::default(),
        })
    }
}