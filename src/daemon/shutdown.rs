use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::error::{Error, Result};

#[derive(Debug, Clone)]
pub struct ShutdownController {
    admission: Arc<AtomicBool>,
    running: CancellationToken,
    deadline: Duration,
}

impl ShutdownController {
    pub fn new(deadline: Duration) -> Self {
        Self {
            admission: Arc::new(AtomicBool::new(true)),
            running: CancellationToken::new(),
            deadline,
        }
    }

    pub fn admission_allowed(&self) -> bool {
        self.admission.load(Ordering::SeqCst)
    }

    pub fn running_token(&self) -> CancellationToken {
        self.running.clone()
    }

    pub fn request_shutdown(&self) {
        self.admission.store(false, Ordering::SeqCst);
        self.running.cancel();
    }

    pub async fn wait_for_cleanup<F>(&self, cleanup: F) -> Result<()>
    where
        F: Future<Output = ()>,
    {
        match tokio::time::timeout(self.deadline, cleanup).await {
            Ok(()) => Ok(()),
            Err(_) => Err(Error::ShutdownDeadline),
        }
    }
}
