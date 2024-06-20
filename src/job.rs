pub struct JobWrapper {
    pub(crate) id: u128,
    pub(crate) name: String,
    pub(crate) job: Box<dyn Job + Send>,
}

impl JobWrapper {
    pub fn new(id: u128, name: String, job: Box<dyn Job + Send>) -> Self {
        Self { id, name, job }
    }
}

pub trait Job {
    fn run(&self) -> Result<(), Box<dyn std::error::Error>>;
}

pub struct SimpleJob(Box<dyn Fn() -> Result<(), Box<dyn std::error::Error>> + Send>);

impl SimpleJob {
    pub fn new<F>(runner: F) -> Self
        where
            F: Fn() -> Result<(), Box<dyn std::error::Error>> + Send + 'static,
    {
        Self(Box::new(runner))
    }
}

impl Job for SimpleJob {
    fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.0()
    }
}
