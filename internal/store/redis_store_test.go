package store

import (
	"context"
	"encoding/json"
	j "github.com/kennybll/queueit/internal/job"
	"github.com/redis/go-redis/v9"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type RedisStoreTestSuite struct {
	suite.Suite
	store *RedisStore
	job1  *j.Job
	job2  *j.Job
	job3  *j.Job
}

func (suite *RedisStoreTestSuite) SetupTest() {
	opts := redis.Options{
		Addr: "localhost:6379",
	}
	suite.store = NewRedisStore(RedisStoreOptions{
		redisOptions: opts,
	})

	err := suite.store.client.FlushDB(context.Background()).Err()
	if err != nil {
		suite.T().Fatalf("Failed to flush Redis DB: %v", err)
	}

	suite.job1 = j.NewJob("data1", j.NewJobOptions{
		Id:          "job1",
		Priority:    3,
		MaxAttempts: 3,
	})
	suite.job2 = j.NewJob("data2", j.NewJobOptions{
		Id:          "job2",
		Priority:    2,
		MaxAttempts: 3,
	})
	suite.job3 = j.NewJob("data3", j.NewJobOptions{
		Id:          "job3",
		Priority:    1,
		MaxAttempts: 3,
	})

	_ = suite.store.Add(suite.job1)
	_ = suite.store.Add(suite.job2)
	_ = suite.store.Add(suite.job3)
}

func (suite *RedisStoreTestSuite) TestAddJob() {
	job := j.NewJob("data4", j.NewJobOptions{
		Priority:    1,
		Delay:       0,
		MaxAttempts: 2,
	})
	err := suite.store.Add(job)
	assert.NoError(suite.T(), err)

	retrievedJob, err := suite.store.GetJob(job.Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), job.Id, retrievedJob.Id)
}

func (suite *RedisStoreTestSuite) TestGetNextJob() {
	nextJob, err := suite.store.GetNextJob()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), suite.job1.Id, nextJob.Id)
}

func (suite *RedisStoreTestSuite) TestGetNextJob_SamePriority() {
	// Update the jobs to have the same priority
	suite.job1.Priority = 1
	suite.job2.Priority = 1
	suite.job3.Priority = 1
	// Update the jobs to have different timestamps
	suite.job1.Timestamp = time.Now().Add(-time.Minute * 3)
	suite.job2.Timestamp = time.Now().Add(-time.Minute * 2)
	suite.job3.Timestamp = time.Now().Add(-time.Minute)

	_ = suite.store.Update(suite.job1)
	_ = suite.store.Update(suite.job2)
	_ = suite.store.Update(suite.job3)

	nextJob, err := suite.store.GetNextJob()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), suite.job1.Id, nextJob.Id)
}

func (suite *RedisStoreTestSuite) TestUpdateJob() {
	suite.job1.Status = j.JobStatusProcessing
	_ = suite.store.Update(suite.job1)

	job, err := suite.store.GetJob(suite.job1.Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), j.JobStatusProcessing, job.Status)
}

func (suite *RedisStoreTestSuite) TestListJobs() {
	jobs, err := suite.store.ListJobs(nil)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), jobs, 3)
}

func (suite *RedisStoreTestSuite) TestGetJobsWithStatus() {
	suite.job1.Status = j.JobStatusProcessing
	_ = suite.store.Update(suite.job1)

	jobs, err := suite.store.GetJobsByStatus(j.JobStatusProcessing)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), jobs, 1)
	assert.Equal(suite.T(), suite.job1.Id, jobs[0].Id)
}

func (suite *RedisStoreTestSuite) TestRetryJob() {
	err := suite.store.RetryJob(suite.job3.Id)
	assert.NoError(suite.T(), err)
	job, err := suite.store.GetJob(suite.job3.Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), j.JobStatusPending, job.Status)
}

func (suite *RedisStoreTestSuite) TestPromoteJob() {
	suite.job3.Status = j.JobStatusDelayed
	suite.job3.Delay = time.Minute
	suite.job3.Timestamp = time.Now().Add(suite.job3.Delay)
	_ = suite.store.Update(suite.job3)

	timeStampCopy := suite.job3.Timestamp

	err := suite.store.PromoteJob(suite.job3.Id)
	assert.NoError(suite.T(), err)
	job, err := suite.store.GetJob(suite.job3.Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), j.JobStatusPending, job.Status)
	assert.NotEqual(suite.T(), timeStampCopy, job.Timestamp)
}

func (suite *RedisStoreTestSuite) TestCountJobsByStatus() {
	count, err := suite.store.CountJobsByStatus(j.JobStatusPending)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), 3, count) // Adjust based on the initial setup
}

func (suite *RedisStoreTestSuite) TestDeleteJob() {
	err := suite.store.Delete(suite.job1.Id)
	assert.NoError(suite.T(), err)
	_, err = suite.store.GetJob(suite.job1.Id)
	assert.Error(suite.T(), err)
}

func (suite *RedisStoreTestSuite) TestNoJobsWhenAllAreInTheFuture() {
	suite.job1.Timestamp = time.Now().Add(time.Minute)
	suite.job2.Timestamp = time.Now().Add(time.Minute)
	suite.job3.Timestamp = time.Now().Add(time.Minute)

	_ = suite.store.Update(suite.job1)
	_ = suite.store.Update(suite.job2)
	_ = suite.store.Update(suite.job3)

	_, err := suite.store.GetNextJob()
	assert.Error(suite.T(), err)
}

func (suite *RedisStoreTestSuite) TestNoJobsWhenAllAreProcessing() {
	suite.job1.Status = j.JobStatusProcessing
	suite.job2.Status = j.JobStatusProcessing
	suite.job3.Status = j.JobStatusProcessing

	_ = suite.store.Update(suite.job1)
	_ = suite.store.Update(suite.job2)
	_ = suite.store.Update(suite.job3)

	_, err := suite.store.GetNextJob()
	assert.Error(suite.T(), err)
}

func (suite *RedisStoreTestSuite) TestNoJobsWhenAllAreDelayed() {
	suite.job1.Status = j.JobStatusDelayed
	suite.job2.Status = j.JobStatusDelayed
	suite.job3.Status = j.JobStatusDelayed

	_ = suite.store.Update(suite.job1)
	_ = suite.store.Update(suite.job2)
	_ = suite.store.Update(suite.job3)

	_, err := suite.store.GetNextJob()
	assert.Error(suite.T(), err)
}

func (suite *RedisStoreTestSuite) TestNoJobsWhenAllAreFailed() {
	suite.job1.Status = j.JobStatusFailed
	suite.job2.Status = j.JobStatusFailed
	suite.job3.Status = j.JobStatusFailed

	_ = suite.store.Update(suite.job1)
	_ = suite.store.Update(suite.job2)
	_ = suite.store.Update(suite.job3)

	_, err := suite.store.GetNextJob()
	assert.Error(suite.T(), err)
}

func (suite *RedisStoreTestSuite) TestNoJobsWhenAllAreCompleted() {
	suite.job1.Status = j.JobStatusCompleted
	suite.job2.Status = j.JobStatusCompleted
	suite.job3.Status = j.JobStatusCompleted

	_ = suite.store.Update(suite.job1)
	_ = suite.store.Update(suite.job2)
	_ = suite.store.Update(suite.job3)

	_, err := suite.store.GetNextJob()
	assert.Error(suite.T(), err)
}

func (suite *RedisStoreTestSuite) TestFailOnRetryWhenHitMaxAttempts() {
	suite.job1.Attempts = 3
	_ = suite.store.Update(suite.job1)

	err := suite.store.RetryJob(suite.job1.Id)
	assert.Error(suite.T(), err)
}

func (suite *RedisStoreTestSuite) TestEncryption() {
	secret := "0123456789abcdef0123456789abcdef"
	opts := redis.Options{
		Addr: "localhost:6379",
	}
	suite.store = NewRedisStore(RedisStoreOptions{
		redisOptions: opts,
		e:            NewEncryption(secret),
	})

	err := suite.store.client.FlushDB(context.Background()).Err()
	if err != nil {
		suite.T().Fatalf("Failed to flush Redis DB: %v", err)
	}

	suite.job1 = j.NewJob("data1", j.NewJobOptions{
		Id:          "job1",
		Priority:    3,
		MaxAttempts: 3,
	})

	err = suite.store.Add(suite.job1)
	assert.NoError(suite.T(), err)

	job, err := suite.store.client.Get(suite.store.ctx, suite.job1.Id).Result()
	assert.NoError(suite.T(), err)

	// Check if the job data is encrypted
	jsonData, err := json.Marshal(job)
	assert.NoError(suite.T(), err)

	assert.NotEqual(suite.T(), jsonData, job)
}

func (suite *RedisStoreTestSuite) SuiteClose() {
	err := suite.store.Close()
	assert.NoError(suite.T(), err)
}

func TestRedisStoreTestSuite(t *testing.T) {
	suite.Run(t, new(RedisStoreTestSuite))
}
