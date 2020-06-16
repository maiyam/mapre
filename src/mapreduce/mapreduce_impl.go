package mapreduce

//
// any additional state that you want to add to type MapReduce
//
type MapReduceImpl struct {
}

//
// additional initialization of mr.* state beyond that in InitMapReduce
//
func (mr *MapReduce) InitMapReduceImpl(nmap int, nreduce int,
	file string, master string) {
}
