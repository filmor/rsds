use std::collections::{HashSet, HashMap};

use crate::scheduler::schedproto::{TaskId, TaskInfo};
use crate::scheduler::implementation::worker::WorkerRef;

pub enum SchedulerTaskState {
    Waiting,
    Ready,
    Finished,
}

pub struct Task {
    pub id: TaskId,
    pub state: SchedulerTaskState,
    pub inputs: Vec<TaskRef>,
    pub consumers: HashSet<TaskRef>,
    pub b_level: f32,
    pub unfinished_deps: u32,
    pub assigned_worker: Option<WorkerRef>,
    pub placement: Vec<WorkerRef>,
}

pub type TaskRef = crate::common::WrappedRcRefCell<Task>;

impl Task {

    #[inline]
    pub fn is_waiting(&self) -> bool {
        match self.state {
            SchedulerTaskState::Waiting => true,
            _ => false
        }
    }

    #[inline]
    pub fn is_ready(&self) -> bool {
        match self.state {
            SchedulerTaskState::Ready => true,
            _ => false
        }
    }
}

impl TaskRef {
    pub fn new(ti: TaskInfo, inputs: Vec<TaskRef>) -> Self {
        let mut unfinished_deps = 0;
        for inp in &inputs {
            let t = inp.get();
            if !t.is_ready() {
                unfinished_deps += 1;
            }
        }
        let task_ref = Self::wrap(Task {
            id: ti.id,
            inputs,
            state: if unfinished_deps != 0 {
                SchedulerTaskState::Waiting
            } else {
                SchedulerTaskState::Ready
            },
            b_level: 0.0,
            unfinished_deps,
            consumers: Default::default(),
            assigned_worker: None,
            placement: Default::default(),
        });
        {
            let task = task_ref.get();
            for inp in &task.inputs {
                let mut t = inp.get_mut();
                t.consumers.insert(task_ref.clone());
            }
        }
        task_ref
    }
}
