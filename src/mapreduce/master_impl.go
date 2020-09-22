package mapreduce

import "fmt"

//
// States to indicate if a worker is busy or idle
const (
	Busy = "busy"
	Idle = "idle"
)

//
// WorkerInfoImpl holds any additional state that you want to add to type WorkerInfo
//
type WorkerInfoImpl struct {
	WorkerStatus string
}

func (mr *MapReduce) handleRegistrations() {
	for {
		select {
		case registeredWorkerName := <-mr.registerChannel:
			DPrintf("Pulled up worker %s from channel in Map section\n", registeredWorkerName)
			if mr.Workers == nil {
				mr.Workers = make(map[string]*WorkerInfo)
			}
			mr.Workers[registeredWorkerName] = &WorkerInfo{registeredWorkerName, WorkerInfoImpl{Idle}}

		case isDone := <-mr.impl.WorkDoneChannel:
			if isDone {
				DPrintf("Exiting Registration Handler in master_impl\n")
				return
			}
		}
	}
}

//
// RunMasterImpl runs the MapReduce job across all the workers
//
func (mr *MapReduce) RunMasterImpl() {

	var i int
	mapChan := make(chan int, mr.nMap)
	reduceChan := make(chan int, mr.nReduce)

	DPrintf("No. of Map Jobs : %d No. of reduce Jobs: %d\n\n\n", mr.nMap, mr.nReduce)
	go mr.handleRegistrations()
	for i = 0; i < mr.nMap; i++ {
		go mr.work(mr.file, i, "Map", mr.nReduce, mapChan)
	}

	for i = 0; i < mr.nMap; i++ {
		DPrintf("%d ) Done with Map job %d\n", i, <-mapChan)
	}

	for i = 0; i < mr.nReduce; i++ {
		go mr.work(mr.file, i, "Reduce", mr.nMap, reduceChan)
	}
	for i = 0; i < mr.nReduce; i++ {
		DPrintf("%d ) Done with Reduce job %d\n", i, <-reduceChan)
	}
	DPrintf("All done with the Map and Reduce jobs!\n")
	mr.impl.WorkDoneChannel <- true
}

//
// Work is a wrapper to invoke workers
//
func (mr *MapReduce) work(fileName string, jobNo int, job JobType, numPhase int, counterChan chan int) {
	DPrintf("invoked local Work function for job: %d, with fileName: %s, and operation: %s\n", jobNo, fileName, job)
	for {
		for _, worker := range mr.Workers {
			mr.impl.mux.Lock()
			if worker.impl.WorkerStatus == Idle {
				worker.impl.WorkerStatus = Busy
				args := &DoJobArgs{fileName, job, jobNo, numPhase}
				var reply DoJobReply

				if ok := call(worker.address, "Worker.DoJob", args, &reply); !ok {
					fmt.Printf("Worker %d did not finish %s job\n", args.JobNumber, args.Operation)
					worker.impl.WorkerStatus = Idle
					mr.impl.mux.Unlock()
				} else {
					fmt.Printf("Worker %d finished %s ok\n", args.JobNumber, args.Operation)
					counterChan <- jobNo
					worker.impl.WorkerStatus = Idle
					mr.impl.mux.Unlock()
					return
				}
			}
		}
	}

}
