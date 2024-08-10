package consumer

// Consumer processes jobs from the queue
import (
	j "github.com/kennybll/queueit/internal/job"
	q "github.com/kennybll/queueit/internal/queue"
	s "github.com/kennybll/queueit/internal/store"
	"time"
)

type Consumer struct {
	queue      *q.Queue
	processJob func(job *j.Job, store s.Store)
	store      s.Store
}

func NewConsumer(queue *q.Queue, store s.Store, processJob func(job *j.Job, store s.Store)) *Consumer {
	return &Consumer{queue: queue, store: store, processJob: processJob}
}

func (c *Consumer) Start() {
	for {
		job, err := c.queue.Dequeue()
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		// Set job status to processing
		job.Status = job.JobStatusProcessing
		// Update job in store
		_ = c.store.Update(job)
		// Process job asynchronously
		go c.processJob(job, c.store)
	}
}
