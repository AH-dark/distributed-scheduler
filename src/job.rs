use job_scheduler::{Job, Uuid};

pub struct JobWrapper<'a> {
    pub(crate) uuid: &'a Uuid,
    pub(crate) name: &'a str,
    pub(crate) job: Job<'a>,
}

impl<'a> JobWrapper<'a> {
    pub fn new(uuid: &'a Uuid, name: &'a str, job: Job<'a>) -> Self {
        Self { uuid, name, job }
    }
}
