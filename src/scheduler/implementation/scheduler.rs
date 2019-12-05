use std::collections::HashMap;

use tokio::sync::mpsc::UnboundedSender;

use crate::scheduler::{ToSchedulerMessage, FromSchedulerMessage};
use crate::scheduler::schedproto::{WorkerId, TaskAssignment, SchedulerRegistration, TaskId, TaskUpdateType, TaskUpdate};
use crate::scheduler::interface::SchedulerComm;
use futures::StreamExt;
use crate::scheduler::implementation::task::{TaskRef, Task, SchedulerTaskState};
use crate::scheduler::implementation::worker::WorkerRef;
use crate::scheduler::implementation::utils::compute_b_level;
use std::time::{Instant, Duration};
use tokio::sync::oneshot;
use tokio::timer::{delay, Delay};
use futures::{future};


pub struct Scheduler {
    network_bandwidth: f32,
    workers: HashMap<WorkerId, WorkerRef>,
    tasks: HashMap<TaskId, TaskRef>,
    ready_to_assign: Vec<TaskRef>,
    new_tasks: Vec<TaskRef>,
}

const MIN_SCHEDULING_DELAY : Duration = Duration::from_millis(15);

impl Scheduler {
    pub fn new() -> Self {
        Scheduler {
            workers: Default::default(),
            tasks: Default::default(),
            ready_to_assign: Default::default(),
            new_tasks: Default::default(),
            network_bandwidth: 100.0, // Guess better default
        }
    }

    fn get_task(&self, task_id: TaskId) -> &TaskRef {
        self.tasks.get(&task_id).unwrap_or_else(|| panic!("Task {} not found", task_id))
    }

    fn get_worker(&self, worker_id: WorkerId) -> &WorkerRef {
        self.workers.get(&worker_id).unwrap_or_else(|| panic!("Worker {} not found", worker_id))
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

/*            let (wake_send, mut wake_recv) = oneshot::channel::<()>();
            let mut wake_send = Some(wake_send);*/
            let mut scheduler_delay : Option<Delay> = None;
            let mut need_scheduling = false;

            while let Some(msgs) = match scheduler_delay {
                None => comm.recv.next().await,
                Some(d) => {
                    match future::select(comm.recv.next(), d).await {
                        futures::future::Either::Left((msgs, d)) => {
                            scheduler_delay = Some(d);
                            msgs
                        }
                        futures::future::Either::Right((_, rcv)) => {
                            scheduler_delay = None;
                            if need_scheduling {
                                need_scheduling = false;
                                self.schedule(&mut comm.send);
                            }
                            rcv.await
                        }
                    }
                }
            } {
                if !self.update(msgs) {
                    if scheduler_delay.is_none() {
                        scheduler_delay = Some(delay(Instant::now() + MIN_SCHEDULING_DELAY));
                        self.schedule(&mut comm.send);
                    } else {
                        need_scheduling = true;
                    }
                }
            }

        /*let message_reader = async {

                while let Some(msgs) = comm.recv.next().await {
                }
            };
            let scheduling = async {
                loop {
                    wake_recv.await;
                    let (s, r) = oneshot::channel::<()>();
                    wake_send = Some(s);
                    wake_recv = r;
                    let when = Instant::now() + Duration::from_millis(100);
                    self.schedule(&mut comm.send);
                    delay(when).await;
                }
            };*/
            //f1.await;
            log::debug!("Scheduler closed");
            Ok(())
    }

    pub fn schedule(&mut self, sender: &mut UnboundedSender<FromSchedulerMessage>) {
        if !self.new_tasks.is_empty() {
            // TODO: utilize information and do not recompute all b-levels
            compute_b_level(&self.tasks);
            self.new_tasks = Vec::new()
        }
        if self.workers.is_empty() {
            return;
        }
        // TODO: Do not sort on every schedule
        //self.ready_to_assign.sort_by(|a, b| a.get().b_level.partial_cmp(&b.get().b_level).unwrap());

    }

    pub fn process_new_tasks(&self, new_tasks: Vec<TaskRef>) {

    }

    fn task_update(&mut self, tu: TaskUpdate) -> bool {
        let mut tref = self.get_task(tu.id).clone();
        let mut task = tref.get_mut();
        match tu.state {
            TaskUpdateType::Placed => {
                let worker = self.get_worker(tu.worker).clone();
                match task.state {
                    SchedulerTaskState::Waiting if task.is_ready() => {
                        task.state = SchedulerTaskState::Finished;
                        let mut invoke_scheduling = false;
                        for tref in &task.consumers {
                            let mut t = tref.get_mut();
                            if t.unfinished_deps <= 1 {
                                assert!(t.unfinished_deps > 0);
                                assert!(t.is_waiting());
                                t.unfinished_deps -= 1;
                                self.ready_to_assign.push(tref.clone());
                                invoke_scheduling = true;
                            } else {
                                t.unfinished_deps -= 1;
                            }
                        }
                        return invoke_scheduling;
                    },
                    SchedulerTaskState::Finished => {
                        /* do nothing extra */
                    },
                    _ => {
                        panic!("Invalid update");
                    }
                };
                task.placement.push(worker);
            },
            TaskUpdateType::Removed => {
                let worker = self.get_worker(tu.worker);
                let index = task.placement.iter().position(|x| x == worker).unwrap();
                task.placement.remove(index);
            },
            TaskUpdateType::Discard => {
                task.placement.clear();
            }
        }
        return false;
    }

    pub fn update(&mut self, messages: Vec<ToSchedulerMessage>) -> bool {
        let mut invoke_scheduling = false;
        for message in messages {
            match message {
                ToSchedulerMessage::TaskUpdate(tu) => {
                    invoke_scheduling |= self.task_update(tu);
                }
                ToSchedulerMessage::NewTask(ti) => {
                    log::debug!("New task {}", ti.id);
                    let task_id = ti.id;
                    let inputs: Vec<_> = ti.inputs.iter().map(|id| self.tasks.get(id).unwrap().clone()).collect();
                    let task = TaskRef::new(ti, inputs);
                    if task.get().is_ready() {
                        self.ready_to_assign.push(task.clone());
                    }
                    self.new_tasks.push(task.clone());
                    assert!(self.tasks.insert(task_id, task).is_none());
                    invoke_scheduling = true;
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

        /*if !new_tasks.is_empty() {
            self.process_new_tasks(new_tasks)
        }*/
        return invoke_scheduling;

        // HACK, random scheduler
        /*if !self.workers.is_empty() {
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
        }*/
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc::unbounded_channel;
    use crate::scheduler::schedproto::{TaskInfo, WorkerInfo};

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

    fn submit_graph1(scheduler: &mut Scheduler) {
       scheduler.update(vec![
           new_task(1, vec![]),
           new_task(2, vec![1]),
           new_task(3, vec![1]),
           new_task(4, vec![2, 3]),
           new_task(5, vec![4]),
           new_task(6, vec![3]),
           new_task(7, vec![6]),
        ]);
    }

    fn connect_workers(scheduler: &mut Scheduler, count: u32, n_cpus: u32) {
        for i in 0..count {
            scheduler.update(vec![
            ToSchedulerMessage::NewWorker(WorkerInfo {
                id: 100 + i as WorkerId,
                n_cpus,
            })]);
        }
    }

    #[test]
    fn test_b_level() {
        let mut sender = make_sender();
        let mut scheduler = Scheduler::new();
        submit_graph1(&mut scheduler);
        assert_eq!(scheduler.ready_to_assign.len(), 1);
        assert_eq!(scheduler.ready_to_assign[0].get().id, 1);
        scheduler.schedule(&mut sender);
        assert_eq!(scheduler.get_task(7).get().b_level, 1.0);
        assert_eq!(scheduler.get_task(6).get().b_level, 2.0);
        assert_eq!(scheduler.get_task(5).get().b_level, 1.0);
        assert_eq!(scheduler.get_task(4).get().b_level, 2.0);
        assert_eq!(scheduler.get_task(3).get().b_level, 3.0);
        assert_eq!(scheduler.get_task(2).get().b_level, 3.0);
        assert_eq!(scheduler.get_task(1).get().b_level, 4.0);
    }

    #[test]
    fn test_worker_1_1() {
        let mut sender = make_sender();
        let mut scheduler = Scheduler::new();
        submit_graph1(&mut scheduler);
        connect_workers(&mut scheduler, 1, 1);
        scheduler.schedule(&mut sender);
    }

}

