use std::collections::HashMap;

use tokio::sync::mpsc::UnboundedSender;

use crate::scheduler::{ToSchedulerMessage, FromSchedulerMessage};
use crate::scheduler::schedproto::{WorkerId, TaskAssignment, SchedulerRegistration, TaskId};
use crate::scheduler::interface::SchedulerComm;
use futures::StreamExt;
use crate::scheduler::implementation::task::{TaskRef, Task};
use crate::scheduler::implementation::worker::WorkerRef;


pub struct Scheduler {
    network_bandwidth: f32,
    workers: HashMap<WorkerId, WorkerRef>,
    tasks: HashMap<TaskId, TaskRef>,

    _tmp_hack: Vec<WorkerId>,
}

impl Scheduler {
    pub fn new() -> Self {
        Scheduler {
            workers: Default::default(),
            tasks: Default::default(),
            network_bandwidth: 100.0, // Guess better default
            _tmp_hack: Vec::new(),
        }
    }

    pub async fn start(mut self, mut comm: SchedulerComm) -> crate::Result<()> {
            log::debug!("Scheduler initialized");

            comm.send
                .try_send(FromSchedulerMessage::Register(SchedulerRegistration {
                    protocol_version: 0,
                    scheduler_name: "test_scheduler".into(),
                    scheduler_version: "0.0".into(),
                    reassigning: false,
                }))
                .expect("Send failed");

            while let Some(msgs) = comm.recv.next().await {
                self.update(msgs, &mut comm.send);
            }
            log::debug!("Scheduler closed");
            Ok(())
    }

    pub fn process_new_tasks(&self, new_tasks: Vec<TaskRef>) {
        /*for tref in new_tasks {
            let task = tref.get();
            if task.consumers().is_empty() {

            }
        }*/
    }

    pub fn update(&mut self, messages: Vec<ToSchedulerMessage>, sender: &mut UnboundedSender<FromSchedulerMessage>) {
        let mut new_tasks: Vec<TaskRef> = Vec::new();
        for message in messages {
            match message {
                ToSchedulerMessage::TaskUpdate(_) => { /* TODO */ }
                ToSchedulerMessage::NewTask(ti) => {
                    log::debug!("New task {}", ti.id);
                    let task_id = ti.id;
                    let inputs: Vec<_> = ti.inputs.iter().map(|id| self.tasks.get(id).unwrap().clone()).collect();
                    let task = TaskRef::new(ti, inputs);
                    new_tasks.push(task.clone());
                    assert!(self.tasks.insert(task_id, task).is_none());
                }
                ToSchedulerMessage::NewWorker(wi) => {
                    assert!(self
                        .workers
                        .insert(
                            wi.id,
                            WorkerRef::new(wi),
                        )
                        .is_none());
                }
                ToSchedulerMessage::NetworkBandwidth(nb) => {
                    self.network_bandwidth = nb;
                }
            }
        }
        if !new_tasks.is_empty() {
            self.process_new_tasks(new_tasks)
        }


        // HACK, random scheduler
        if !self.workers.is_empty() {
            use rand::seq::SliceRandom;
            let mut result = Vec::new();
            let mut rng = rand::thread_rng();
            let ws: Vec<WorkerId> = self.workers.values().map(|w| w.get().id).collect();
            // TMP HACK
            for task_id in &self._tmp_hack {
                result.push(TaskAssignment {
                    task: *task_id,
                    worker: *ws.choose(&mut rng).unwrap(),
                    priority: 0,
                });
            }
            self._tmp_hack.clear();

            sender
                .try_send(FromSchedulerMessage::TaskAssignments(result))
                .unwrap();
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc::unbounded_channel;
    use crate::scheduler::schedproto::TaskInfo;

    /* Graph1
         T1
        /  \
       T2   T3
       |  / |\
       T4   | T6
        \      \
         \ /   T7
          T5

    */

    fn make_sender() -> UnboundedSender<FromSchedulerMessage> {
        let (sender, _) = unbounded_channel::<FromSchedulerMessage>();
        sender
    }

    fn new_task(id: TaskId, inputs: Vec<TaskId>) -> ToSchedulerMessage {
        ToSchedulerMessage::NewTask(TaskInfo {
                id: id,
                inputs: inputs
        })
    }

    fn submit_graph1(scheduler: &mut Scheduler, mut sender: &mut UnboundedSender<FromSchedulerMessage>) {
       scheduler.update(vec![
           new_task(1, vec![]),
           new_task(2, vec![1]),
           new_task(3, vec![1]),
           new_task(4, vec![2, 3]),
           new_task(5, vec![4]),
           new_task(6, vec![3]),
           new_task(7, vec![6]),
        ], &mut sender);
    }

    #[test]
    fn test_new_tasks() {
        let mut sender = make_sender();
        let mut scheduler = Scheduler::new();
        submit_graph1(&mut scheduler, &mut sender);
    }
}

