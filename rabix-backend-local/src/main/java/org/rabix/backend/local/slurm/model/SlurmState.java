package org.rabix.backend.local.slurm.model;

public enum SlurmState {
    Unknown,
    Queued,
    Running,
    Paused,
    Completed,
    Error,
    SystemError,
    Canceled,
    Initializing
}