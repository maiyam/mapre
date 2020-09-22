package mapreduce

import "sync"

//
// any additional state that you want to add to type MapReduce
//
type MapReduceImpl struct {
	WorkDoneChannel chan bool
	mux             sync.Mutex
}

//
// additional initialization of mr.* state beyond that in InitMapReduce
//
func (mr *MapReduce) InitMapReduceImpl(nmap int, nreduce int,
	file string, master string) {
	mr.impl.WorkDoneChannel = make(chan bool)
}
