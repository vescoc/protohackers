use std::cell::RefCell;
use std::collections::{BinaryHeap, HashMap};
use std::rc::{Rc, Weak};

use wasi_async_runtime::sync::Notify;

use thiserror::Error;

use tracing::{debug, warn};

type Job = serde_json::Value;
type JobId = u32;

#[derive(Debug, PartialEq, Clone)]
pub struct JobRef(pub JobId, pub Job, pub u32, pub String);

#[derive(Error, Debug, PartialEq)]
pub enum WorkerError {
    #[error("no-job")]
    NoJob,

    #[error("invalid request")]
    InvalidRequest,
}

/// The worker.
pub struct Worker {
    job_centre: JobCentre,
    jobs: Vec<Weak<JobRef>>,
}

impl Worker {
    /// Insert the given job into the given queue with the given
    /// priority.
    #[must_use]
    pub fn put(&mut self, queue: String, job: Job, priority: u32) -> JobId {
        debug!("worker put");
        self.job_centre.put(queue, job, priority)
    }

    /// Retrieve the highest-priority job that is currently waiting in
    /// any of the listed queues, and remove it from its queue.
    ///
    /// # Errors
    ///
    /// - when no-job and `wait` is `false`
    pub async fn get<Q: AsRef<str>>(
        &mut self,
        queues: &[Q],
        wait: bool,
    ) -> Result<JobRef, WorkerError> {
        debug!("worker get wait: {wait}");
        let job_ref = self.job_centre.get(queues, wait).await?;
        self.jobs.push(job_ref.clone());
        if let Some(mut job_ref) = job_ref.upgrade() {
            Ok(Rc::make_mut(&mut job_ref).clone())
        } else {
            warn!("job deallocated");
            Err(WorkerError::NoJob)
        }
    }

    /// Delete the job with the given `id`, so that it can never be
    /// retrieved, aborted, or deleted already again. Valid from any
    /// client.
    ///
    /// # Errors
    ///
    /// - when no-job
    pub fn delete(&mut self, id: JobId) -> Result<(), WorkerError> {
        debug!("worker delete");
        self.job_centre.delete(id)
    }

    /// Put the job with the given `id` back in its queue. This
    /// request is only valid from the client that is currently
    /// working on that job. It is an error for any client to attempt
    /// to abort a job that is not currently working on.
    ///
    /// # Errors
    ///
    /// - when requesting abort of a not-owned job
    pub fn abort(&mut self, id: JobId) -> Result<(), WorkerError> {
        debug!("worker abort");
        if let Some(job_ref) = self.get_from_working_jobs(id) {
            self.job_centre.abort(&job_ref);
            Ok(())
        } else {
            Err(WorkerError::InvalidRequest)
        }
    }

    fn get_from_working_jobs(&mut self, job_id: JobId) -> Option<Rc<JobRef>> {
        let mut r = None;
        self.jobs.retain_mut(|job_ref| {
            if let Some(job_ref) = job_ref.upgrade() {
                if job_ref.0 == job_id {
                    r = Some(job_ref.clone());
                    return false;
                }
                true
            } else {
                false
            }
        });
        r
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        self.job_centre.remove_worker(&self.jobs);
    }
}

struct JobCentreInner {
    next_job_id: JobId,
    queues: HashMap<String, BinaryHeap<(u32, JobId)>>,
    jobs: HashMap<JobId, (Job, u32, String)>,
    running_jobs: Vec<Rc<JobRef>>,
}

impl JobCentreInner {
    fn next_job_id(&mut self) -> u32 {
        let r = self.next_job_id;
        self.next_job_id += 1;
        r
    }

    fn put(&mut self, queue: String, job: Job, priority: u32) -> u32 {
        debug!("jobcentreinner put");
        let job_id = self.next_job_id();
        self.jobs.insert(job_id, (job, priority, queue.clone()));
        let queue = self.queues.entry(queue).or_default();
        queue.push((priority, job_id));
        job_id
    }

    fn get<Q: AsRef<str>>(&mut self, queues: &[Q]) -> Result<Weak<JobRef>, WorkerError> {
        debug!("jobcentreinner get");
        let r = queues
            .iter()
            .filter_map(|queue_name| {
                if let Some(queue) = self.queues.get(queue_name.as_ref())
                    && let Some((priority, _)) = queue.peek()
                {
                    return Some((*priority, queue_name));
                }
                None
            })
            .max_by_key(|(priority, _)| *priority);

        if let Some((_, queue)) = r {
            let (_, id) = self.queues.get_mut(queue.as_ref()).unwrap().pop().unwrap();
            let (job, priority, queue) = self.jobs.get(&id).unwrap();
            let job_ref = Rc::new(JobRef(id, job.clone(), *priority, queue.clone()));
            let r = Rc::downgrade(&job_ref);
            self.running_jobs.push(job_ref);
            return Ok(r);
        }

        Err(WorkerError::NoJob)
    }

    fn delete(&mut self, job_id: JobId) -> Result<(), WorkerError> {
        if let Some(job) = self.jobs.remove(&job_id) {
            self.running_jobs.retain(|job_ref| job_ref.0 != job_id);
            if let Some(queue) = self.queues.get_mut(&job.2) {
                queue.retain(|(_, id)| *id != job_id);
            }
            Ok(())
        } else {
            Err(WorkerError::NoJob)
        }
    }

    fn abort(&mut self, job_ref: &Rc<JobRef>) {
        self.running_jobs
            .retain(|running_job_ref| running_job_ref.0 != job_ref.0);
        let queue = self.queues.entry(job_ref.3.clone()).or_default();
        queue.push((job_ref.2, job_ref.0));
    }

    fn remove_worker(&mut self, job_refs: &[Weak<JobRef>]) {
        for job_ref in job_refs {
            if let Some(job_ref) = job_ref.upgrade() {
                self.abort(&job_ref);
            }
        }
    }
}

/// The Job Centre
#[derive(Clone)]
pub struct JobCentre(Rc<(RefCell<JobCentreInner>, Notify)>);

impl Default for JobCentre {
    fn default() -> Self {
        Self::new()
    }
}

impl JobCentre {
    #[must_use]
    pub fn new() -> Self {
        Self(Rc::new((
            RefCell::new(JobCentreInner {
                next_job_id: 1,
                queues: HashMap::new(),
                jobs: HashMap::new(),
                running_jobs: vec![],
            }),
            Notify::new(),
        )))
    }

    #[must_use]
    pub fn make_worker(&self) -> Worker {
        Worker {
            job_centre: self.clone(),
            jobs: vec![],
        }
    }

    fn remove_worker(&self, job_refs: &[Weak<JobRef>]) {
        self.0.0.borrow_mut().remove_worker(job_refs);
        self.0.1.notify_waiters();
    }

    fn put(&self, queue: String, job: Job, priority: u32) -> u32 {
        debug!("jobcentre put");
        let r = self.0.0.borrow_mut().put(queue, job, priority);
        self.0.1.notify_waiters();
        r
    }

    async fn get<'a, Q: AsRef<str> + 'a>(
        &'a self,
        queues: &'a [Q],
        wait: bool,
    ) -> Result<Weak<JobRef>, WorkerError> {
        loop {
            let r = { self.0.0.borrow_mut().get(queues) };
            debug!("jobcentre get {r:?} wait: {wait}");
            if r.is_err() && wait {
                self.0.1.notified().await;
            } else {
                break r;
            }
        }
    }

    fn delete(&self, id: JobId) -> Result<(), WorkerError> {
        self.0.0.borrow_mut().delete(id)
    }

    fn abort(&self, job_ref: &Rc<JobRef>) {
        self.0.0.borrow_mut().abort(job_ref);
        self.0.1.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use std::pin;
    use std::time::Duration;

    use serde_json::json;

    use wasi_async::time::{sleep, timeout};
    use wasi_async_runtime::Reactor;

    use super::*;

    const TIMEOUT: Duration = Duration::from_millis(100);
    const TIMEOUT_2: Duration = Duration::from_millis(200);

    #[test]
    fn test_put() {
        let job_centre = JobCentre::new();

        let mut worker = job_centre.make_worker();

        let id = worker.put("queue1".to_string(), json! {{"name": "job"}}, 123);
        assert_eq!(id, 1);
    }

    #[test]
    fn test_get() {
        Reactor::block_on(|_| async {
            let job_centre = JobCentre::new();

            let mut worker = job_centre.make_worker();

            let id1 = worker.put("queue1".to_string(), json! {{"name": "job1"}}, 123);
            let id2 = worker.put("queue1".to_string(), json! {{"name": "job2"}}, 100);

            let JobRef(target_id, _, target_priority, target_queue) =
                worker.get(&["queue1", "queue2"], false).await.unwrap();
            assert_eq!(id1, target_id);
            assert_eq!(123, target_priority);
            assert_eq!("queue1", target_queue);

            let job_ref = worker.get(&["queue1", "queue2"], false).await.unwrap();
            assert_eq!(id2, job_ref.0);
            assert_eq!(100, job_ref.2);
            assert_eq!("queue1", job_ref.3);

            assert_eq!(
                worker.get(&["queue1", "queue2"], false).await.unwrap_err(),
                WorkerError::NoJob
            );
        });
    }

    #[test]
    fn test_wait_get() {
        Reactor::block_on(|_| async {
            let job_centre = JobCentre::new();

            let mut worker = job_centre.make_worker();
            {
                let w1 = pin::pin!(worker.get(&["queue1", "queue2"], false));
                assert_eq!(
                    timeout(TIMEOUT, w1).await.unwrap().unwrap_err(),
                    WorkerError::NoJob
                );
            }

            let id1 = worker.put("queue1".to_string(), json! {{"name": "job1"}}, 123);

            let job_ref = worker.get(&["queue1", "queue2"], false).await.unwrap();
            assert_eq!(id1, job_ref.0);
            assert_eq!(123, job_ref.2);
            assert_eq!("queue1", job_ref.3);

            let handle = Reactor::spawn_in_current(async move {
                {
                    let w1 = pin::pin!(worker.get(&["queue1", "queue2"], true));
                    assert!(timeout(TIMEOUT, w1).await.is_err());
                }

                {
                    let w2 = pin::pin!(worker.get(&["queue1", "queue2"], true));
                    timeout(TIMEOUT_2, w2).await.unwrap()
                }
            })
            .await;

            sleep(TIMEOUT_2).await;

            let mut worker = job_centre.make_worker();

            let id2 = worker.put("queue1".to_string(), json! {{"name": "job2"}}, 100);

            let job_ref = handle.await.unwrap();

            assert_eq!(id2, job_ref.0);
            assert_eq!(100, job_ref.2);
            assert_eq!("queue1", job_ref.3);
        });
    }

    #[test]
    fn test_delete() {
        let job_centre = JobCentre::new();

        let mut worker = job_centre.make_worker();

        let id = worker.put("queue1".to_string(), json! {{"name": "job"}}, 123);

        assert_eq!(Ok(()), worker.delete(id));
        assert_eq!(Err(WorkerError::NoJob), worker.delete(id));
    }

    #[test]
    fn test_delete_from_other_worker() {
        Reactor::block_on(|_| async {
            let job_centre = JobCentre::new();

            let mut worker1 = job_centre.make_worker();

            let id = worker1.put("queue1".to_string(), json! {{"name": "job"}}, 123);
            let _ = worker1.get(&["queue1"], false).await.unwrap();

            let mut worker2 = job_centre.make_worker();
            assert_eq!(Ok(()), worker2.delete(id));
        });
    }

    #[test]
    fn test_abort() {
        Reactor::block_on(|_| async {
            let job_centre = JobCentre::new();

            let mut worker = job_centre.make_worker();

            let id = worker.put("queue1".to_string(), json! {{"name": "job"}}, 123);
            let _ = worker.get(&["queue1"], false).await.unwrap();

            assert_eq!(Ok(()), worker.abort(id));
            assert_eq!(Err(WorkerError::InvalidRequest), worker.abort(id));
        });
    }

    #[test]
    fn test_abort_from_other_worker() {
        Reactor::block_on(|_| async {
            let job_centre = JobCentre::new();

            let mut worker1 = job_centre.make_worker();

            let id = worker1.put("queue1".to_string(), json! {{"name": "job"}}, 123);
            let _ = worker1.get(&["queue1"], false).await.unwrap();

            let mut worker2 = job_centre.make_worker();
            assert_eq!(Err(WorkerError::InvalidRequest), worker2.abort(id));

            assert_eq!(Ok(()), worker1.abort(id));
        });
    }
}
