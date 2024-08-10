package queue

import (
	j "github.com/kennybll/queueit/internal/job"
	"github.com/kennybll/queueit/internal/store"
	"sync"
)

type Queue struct {
	store store.Store
	mu    sync.Mutex
}

func NewQueue(store store.Store) *Queue {
	return &Queue{store: store}
}

func (q *Queue) Enqueue(job *j.Job) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.store.Add(job)
}

func (q *Queue) Dequeue() (*j.Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	job, err := q.store.GetNextJob()
	if err != nil {
		return nil, err
	}
	return job, nil
}
