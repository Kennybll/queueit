package main

import (
	"encoding/json"
	"fmt"
	c "github.com/kennybll/queueit/internal/consumer"
	j "github.com/kennybll/queueit/internal/job"
	p "github.com/kennybll/queueit/internal/producer"
	q "github.com/kennybll/queueit/internal/queue"
	store2 "github.com/kennybll/queueit/internal/store"
	"log"
)

func main() {
	// Initialize store
	store := store2.NewMemoryStore()

	// Initialize queue with store
	queue := q.NewQueue(store)

	// Initialize producer, consumer
	producer := p.NewProducer(queue)
	consumer := c.NewConsumer(queue, store, func(job *j.Job, store store2.Store) {
		// Process job
		println("Processing job", job.Id)
		job.Fail()
		_ = store.Update(job)
		// Print out the job as json
		printJobAsJSON(job)
	})

	// Start the consumer and scheduler
	go consumer.Start()

	// Example of producing a job
	job := j.NewJob("example", j.NewJobOptions{
		MaxAttempts: 4,
	})
	err := producer.Produce(job)

	if err != nil {
		println("Error producing job", err)
	}

	// Keep the main function running
	select {}
}

func printJobAsJSON(j *j.Job) {
	jobData, err := json.MarshalIndent(j, "", "    ")
	if err != nil {
		log.Fatalf("Error marshaling job to JSON: %v", err)
	}
	fmt.Println(string(jobData))
}
