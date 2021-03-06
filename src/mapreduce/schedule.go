package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	var wg sync.WaitGroup
	idxChan := make(chan int, ntasks)
	for i := 0; i < ntasks; i++ {
		wg.Add(1)
		idxChan <- i
	}
	debug("ntasks = %d\n", ntasks)
	go func() {
		for {
			num := <-idxChan
			go func(num int) {
				debug("Fetching No.%d addr\n", num)
				address := <-registerChan
				debug("No.%d addr fetched\n", num)
				args := DoTaskArgs{jobName, mapFiles[num], phase, num, n_other}
				res := call(address, "Worker.DoTask", args, nil)
				debug("Task %d finished\n", num)
				if res == true {
					wg.Done()
					debug("Task %d done\n", num)
					registerChan <- address
				} else {
					debug("Task %d failed\n", num)
					idxChan <- num
				}
				debug("address %s returned to channel\n", address)
			}(num)
		}
	}()
	debug("All tasks sent, waiting for finish\n")
	wg.Wait()
	fmt.Printf("Schedule: %v done\n", phase)
}
