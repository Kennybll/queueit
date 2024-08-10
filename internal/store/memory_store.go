package store

import (
	"fmt"
	j "github.com/kennybll/queueit/internal/job"
	"time"
)

type MemoryStore struct {
	data map[string]*j.Job
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		data: make(map[string]*j.Job),
	}
}

func (s *MemoryStore) Add(job *j.Job) error {
	s.data[job.Id] = job

	return nil
}

func (s *MemoryStore) GetNextJob() (*j.Job, error) {
	var nextJob *j.Job
	for _, job := range s.data {
		// Compare job priority and timestamp to determine the next job
		// Priority overrides timestamp
		if job.Status == job.JobStatusPending && job.Timestamp.Before(time.Now()) {
			if nextJob == nil || job.Priority > nextJob.Priority || (job.Priority == nextJob.Priority && job.Timestamp.Before(nextJob.Timestamp)) {
				nextJob = job
			}
		}
	}

	if nextJob == nil {
		return nil, fmt.Errorf("no pending jobs found")
	}

	return nextJob, nil
}

func (s *MemoryStore) GetJob(id string) (*j.Job, error) {
	job, ok := s.data[id]
	if !ok {
		return nil, fmt.Errorf("job not found")
	}

	return job, nil
}

func (s *MemoryStore) Delete(id string) error {
	delete(s.data, id)

	return nil
}

func (s *MemoryStore) Update(job *j.Job) error {
	s.data[job.Id] = job

	return nil
}

func (s *MemoryStore) ListJobs(status *j.JobStatus) ([]*j.Job, error) {
	var jobs []*j.Job
	for _, job := range s.data {
		if status == nil || job.Status == *status {
			jobs = append(jobs, job)
		}
	}

	return jobs, nil
}

func (s *MemoryStore) RetryJob(id string) error {
	job, err := s.GetJob(id)
	if err != nil {
		return err
	}

	if !job.Retry() {
		return fmt.Errorf("no more attempts left")
	}

	return s.Update(job)
}

func (s *MemoryStore) PromoteJob(id string) error {
	job, err := s.GetJob(id)
	if err != nil {
		return err
	}

	job.Promote()
	return s.Update(job)
}

func (s *MemoryStore) GetJobsByStatus(status j.JobStatus) ([]*j.Job, error) {
	return s.ListJobs(&status)
}

func (s *MemoryStore) CountJobsByStatus(status j.JobStatus) (int, error) {
	jobs, err := s.GetJobsByStatus(status)
	if err != nil {
		return 0, err
	}
	return len(jobs), nil
}

func (s *MemoryStore) Close() error {
	return nil
}
