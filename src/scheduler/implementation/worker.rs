use crate::scheduler::schedproto::{WorkerId, WorkerInfo};

pub struct Worker {
    pub id: WorkerId,
    pub ncpus: u32,
    pub free_cpus: i32,
}

pub type WorkerRef = crate::common::WrappedRcRefCell<Worker>;

impl WorkerRef {
    pub fn new(wi: WorkerInfo) -> Self {
        Self::wrap(Worker {
            id: wi.id,
            ncpus: wi.ncpus,
            free_cpus: wi.ncpus as i32,
        })
    }
}