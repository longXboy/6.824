package mapreduce

import (
	"fmt"
	"sync"
	"time"
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
type Recycle struct {
	addr    string
	success bool
}

func doTask(arg DoTaskArgs, workerCh chan string, recycleCh chan Recycle, group *sync.WaitGroup) {
	for {
		addr := <-workerCh
		success := call(addr, "Worker.DoTask", &arg, nil)
		r := Recycle{addr: addr, success: success}
		recycleCh <- r
		if success {
			break
		}
	}
	group.Done()
}

func dispatch(registerChan chan string, workerCh chan string, recycleCh chan Recycle, c chan struct{}) {
	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()
	recycles := make([]string, 0)
	for {
		select {
		case _ = <-ticker.C:
			if len(recycles) != 0 {
				workerCh <- recycles[0]
				recycles = recycles[1:]
			}
		case addr := <-registerChan:
			workerCh <- addr
		case r := <-recycleCh:
			if r.success {
				workerCh <- r.addr
			} else {
				recycles = append(recycles, r.addr)
			}
		case <-c:
			return
		}
	}

}

func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	fmt.Println(" in phase")
	sig := make(chan struct{})
	group := &sync.WaitGroup{}
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
		workerCh := make(chan string, ntasks)
		recycleCh := make(chan Recycle, ntasks)

		go dispatch(registerChan, workerCh, recycleCh, sig)
		for taskNo := range mapFiles {
			group.Add(1)
			var arg DoTaskArgs = DoTaskArgs{
				JobName:       jobName,
				File:          mapFiles[taskNo],
				Phase:         mapPhase,
				TaskNumber:    taskNo,
				NumOtherPhase: n_other,
			}
			go doTask(arg, workerCh, recycleCh, group)
		}
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
		workerCh := make(chan string, ntasks)
		recycleCh := make(chan Recycle, ntasks)

		go dispatch(registerChan, workerCh, recycleCh, sig)
		for taskNo := 0; taskNo < nReduce; taskNo++ {
			group.Add(1)
			var arg DoTaskArgs = DoTaskArgs{
				JobName:       jobName,
				File:          "",
				Phase:         reducePhase,
				TaskNumber:    taskNo,
				NumOtherPhase: n_other,
			}
			go doTask(arg, workerCh, recycleCh, group)
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
