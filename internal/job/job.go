package job

import (
	"github.com/google/uuid"
	"sync"
	"time"
)

// JobStatus represents the status of a job.
type JobStatus int

const (
	JobStatusPending JobStatus = iota
	JobStatusDelayed
	JobStatusProcessing
	JobStatusCompleted
	JobStatusFailed
)

// Job represents a unit of work to be processed.
type Job struct {
	Id          string        `json:"id"`
	Data        string        `json:"data"`
	Priority    int           `json:"priority"`
	Delay       time.Duration `json:"delay"`
	Timestamp   time.Time     `json:"timestamp"`
	Status      JobStatus     `json:"status"`
	Attempts    int           `json:"attempts"`
	MaxAttempts int           `json:"max_attempts"`

	mu sync.RWMutex
}

// NewJob creates a new Job with the given data and options.
func NewJob(data string, options NewJobOptions) *Job {
	id := options.Id
	if id == "" {
		id = uuid.New().String()
	}

	priority := options.Priority
	delay := options.Delay
	startTime := time.Now()
	status := JobStatusPending
	if delay > 0 {
		startTime = startTime.Add(delay)
		status = JobStatusDelayed
	}

	if !options.Start.IsZero() {
		startTime = options.Start
	}

	return &Job{
		Id:          id,
		Data:        data,
		Priority:    priority,
		Delay:       delay,
		Timestamp:   startTime,
		Status:      status,
		MaxAttempts: options.MaxAttempts,
	}
}

// NewJobOptions provides options for creating a new Job.
type NewJobOptions struct {
	Id          string
	Priority    int
	Delay       time.Duration
	Start       time.Time
	MaxAttempts int
}

func (j *Job) Retry() bool {
	j.mu.Lock()
	defer j.mu.Unlock()

	j.Attempts++

	// We are not allowed to retry
	if j.MaxAttempts == 0 {
		return false
	}

	// We have reached the maximum number of attempts
	if j.Attempts >= j.MaxAttempts {
		return false
	}

	j.Timestamp = time.Now()
	j.Status = JobStatusPending
	j.Delay = 0
	return true
}

func (j *Job) Promote() {
	j.mu.Lock()
	defer j.mu.Unlock()

	j.Timestamp = time.Now()
	j.Status = JobStatusPending
	j.Delay = 0
}

func (j *Job) Complete() {
	j.mu.Lock()
	defer j.mu.Unlock()

	j.Status = JobStatusCompleted
}

func (j *Job) Fail() {
	if !j.Retry() {
		j.Status = JobStatusFailed
	}
}
