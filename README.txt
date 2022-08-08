tsqueue: A Thread-safe Queue

Paul Krueger

In this implementation multiple readers and multiple writers can access a FIFO queue simultaneously from any number of threads. Readers never block writers and writers never block readers. For example, in an application where there are multiple writers and one reader, the reader will never block (unless there is nothing in the queue). Timeouts can be set to limit block time. If queue processing is of limited duration, the initiator of the process can wait until processing is complete if well-behaved readers and writers indicate when they are done. An example of this sort of operation is provided in the source code and can be exercised by executing (tsq::test-tsq).