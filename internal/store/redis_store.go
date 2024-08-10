package store

import (
	"context"
	"encoding/json"
	"fmt"
	j "github.com/kennybll/queueit/internal/job"
	"github.com/redis/go-redis/v9"
	"time"
)

type RedisStore struct {
	client *redis.Client
	ctx    context.Context
}

func NewRedisStore(opts redis.Options) *RedisStore {
	return &RedisStore{
		client: redis.NewClient(&opts),
		ctx:    context.Background(),
	}
}

// Add adds a job to the Redis store.
func (s *RedisStore) Add(job *j.Job) error {
	jobData, err := json.Marshal(job)
	if err != nil {
		return err
	}

	return s.client.Set(s.ctx, job.Id, jobData, 0).Err()
}

// GetNextJob retrieves the next job to be processed based on priority or timestamp.
func (s *RedisStore) GetNextJob() (*j.Job, error) {
	// Retrieve all job IDs from Redis
	keys, err := s.client.Keys(s.ctx, "*").Result()
	if err != nil {
		return nil, err
	}

	var nextJob *j.Job
	for _, key := range keys {
		job, err := s.GetJob(key)
		if err != nil {
			continue
		}

		if job.Status == job.JobStatusPending && job.Timestamp.Before(time.Now()) {
			// Compare job priority and timestamp to determine the next job
			// Priority overrides timestamp
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

// GetJob retrieves a job from the Redis store by ID.
func (s *RedisStore) GetJob(id string) (*j.Job, error) {
	jobData, err := s.client.Get(s.ctx, id).Result()
	if err != nil {
		return nil, err
	}

	var job j.Job
	if err := json.Unmarshal([]byte(jobData), &job); err != nil {
		return nil, err
	}

	return &job, nil
}

// Delete removes a job from the Redis store by ID.
func (s *RedisStore) Delete(id string) error {
	return s.client.Del(s.ctx, id).Err()
}

// Update updates the job details in the Redis store.
func (s *RedisStore) Update(job *j.Job) error {
	jobData, err := json.Marshal(job)
	if err != nil {
		return err
	}

	return s.client.Set(s.ctx, job.Id, jobData, 0).Err()
}

// ListJobs lists all job or job filtered by status.
func (s *RedisStore) ListJobs(status *j.JobStatus) ([]*j.Job, error) {
	keys, err := s.client.Keys(s.ctx, "*").Result()
	if err != nil {
		return nil, err
	}

	var jobs []*j.Job
	for _, key := range keys {
		job, err := s.GetJob(key)
		if err != nil {
			continue
		}

		if status == nil || job.Status == *status {
			jobs = append(jobs, job)
		}
	}

	return jobs, nil
}

// RetryJob retries a job by updating its status and attempts.
func (s *RedisStore) RetryJob(id string) error {
	job, err := s.GetJob(id)
	if err != nil {
		return err
	}

	if !job.Retry() {
		return fmt.Errorf("no more attempts left")
	}

	return s.Update(job)
}

// PromoteJob promotes a job to be processed immediately.
func (s *RedisStore) PromoteJob(id string) error {
	job, err := s.GetJob(id)
	if err != nil {
		return err
	}

	job.Promote()
	return s.Update(job)
}

// GetJobsByStatus retrieves job by their status.
func (s *RedisStore) GetJobsByStatus(status j.JobStatus) ([]*j.Job, error) {
	return s.ListJobs(&status)
}

// CountJobsByStatus counts the number of job by their status.
func (s *RedisStore) CountJobsByStatus(status j.JobStatus) (int, error) {
	jobs, err := s.GetJobsByStatus(status)
	if err != nil {
		return 0, err
	}
	return len(jobs), nil
}

// Close closes the Redis client connection.
func (s *RedisStore) Close() error {
	return s.client.Close()
}
