use std::collections::HashMap;
use crate::scheduler::schedproto::TaskId;
use crate::scheduler::implementation::task::TaskRef;

pub fn compute_b_level(tasks: HashMap<TaskId, TaskRef>) {
    let mut n_consumers : HashMap<TaskRef, u32> = HashMap::new();
    let mut stack: Vec<TaskRef> = Vec::new();
    for (id, tref) in &tasks {
        let len = tref.get().consumers.len() as u32;
        n_consumers.insert(tref.clone(), len);
        if len == 0 {
            //tref.get_mut().b_level = 0.0;
            stack.push(tref.clone());
        }
    }
    loop {
        let tref = match stack.pop() {
            None => break,
            Some(tref) => tref,
        };
        let mut task = tref.get_mut();

        let mut b_level = 0.0f32;
        for tr in &task.consumers {
            b_level = b_level.max(tr.get().b_level);
        }
        task.b_level = b_level + 1.0f32;

        for inp in &task.inputs {
            let v : &u32 = word_map.get_mut(&inp).unwrap();

        }
    }
}