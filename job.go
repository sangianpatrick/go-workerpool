package workerpool

// Job represents a unit of work to be processed by the worker pool.
type Job struct {
	ID   string
	Data any
}
