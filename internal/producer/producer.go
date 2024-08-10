package producer

import (
	j "github.com/kennybll/queueit/internal/job"
	q "github.com/kennybll/queueit/internal/queue"
)

// Producer adds jobs to the queue
type Producer struct {
	queue *q.Queue
}

func NewProducer(queue *q.Queue) *Producer {
	return &Producer{queue: queue}
}

func (p *Producer) Produce(job *j.Job) error {
	return p.queue.Enqueue(job)
}
