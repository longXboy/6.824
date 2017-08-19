package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
type Compelete struct {
	addr    string
	success bool
}

func doTask(arg DoTaskArgs, workerCh chan string, addr string, group *sync.WaitGroup) {
	for {
		sucess := call(addr, "Worker.DoTask", &arg, nil)
		workerCh <- addr
		if sucess {
			break
		}
	}
	group.Done()
}

func dispatch(registerChan chan string, workerCh chan string, c chan struct{}) {
	for {
		select {
		case addr := <-registerChan:
			workerCh <- addr
		case <-c:
			return
		}
	}

}

func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	sig := make(chan struct{})
	group := &sync.WaitGroup{}
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
		workerCh := make(chan string, ntasks)
		go dispatch(registerChan, workerCh, sig)
		for taskNo := range mapFiles {
			group.Add(1)
			addr := <-workerCh
			var arg DoTaskArgs = DoTaskArgs{
				JobName:       jobName,
				File:          mapFiles[taskNo],
				Phase:         mapPhase,
				TaskNumber:    taskNo,
				NumOtherPhase: n_other,
			}
			go doTask(arg, workerCh, addr, group)
		}
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
		workerCh := make(chan string, ntasks)
		go dispatch(registerChan, workerCh, sig)
		for taskNo := 0; taskNo < nReduce; taskNo++ {
			group.Add(1)
			addr := <-workerCh
			var arg DoTaskArgs = DoTaskArgs{
				JobName:       jobName,
				File:          "",
				Phase:         reducePhase,
				TaskNumber:    taskNo,
				NumOtherPhase: n_other,
			}
			go doTask(arg, workerCh, addr, group)
		}
	}
	group.Wait()
	close(sig)
	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)
	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	fmt.Printf("Schedule: %v phase done\n", phase)
}
