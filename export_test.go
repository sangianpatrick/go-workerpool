package workerpool

// ForceCloseChannel is a test-only helper that closes the job channel directly.
// It is used to simulate the race condition where Submit sends to a closed channel,
// triggering the recover path. This function is only available during testing
// (compiled via _test.go convention) and must NOT be used in production code.
func (wp *WorkerPool) ForceCloseChannel() {
	close(wp.config.jobChan)
	wp.config.wg.Wait()
}
