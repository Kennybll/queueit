package store

import (
	j "github.com/kennybll/queueit/internal/job"
)

type Store interface {
	Add(job *j.Job) error
	GetNextJob() (*j.Job, error)
	GetJob(id string) (*j.Job, error)
	Delete(id string) error
	Update(job *j.Job) error
	ListJobs(status *j.JobStatus) ([]*j.Job, error)
	RetryJob(id string) error
	PromoteJob(id string) error
	GetJobsByStatus(status j.JobStatus) ([]*j.Job, error)
	CountJobsByStatus(status j.JobStatus) (int, error)

	Close() error
}
