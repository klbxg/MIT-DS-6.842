package mapreduce

import "container/list"
import "fmt"


type WorkerInfo struct {
	address string
	// You can add definitions here.
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	var mapChannel, reduceChannel = make(chan bool, mr.nMap), make(chan bool, mr.nReduce)

	for i := 0; i < mr.nMap; i++ {
		go func (jobN int) {
			var worker string
			var jobArgs DoJobArgs
			var reply DoJobReply
			jobArgs.File = mr.file
			jobArgs.Operation = Map
			jobArgs.JobNumber = jobN
			jobArgs.NumOtherPhase = mr.nReduce
			for {
				select {
				case worker = <- mr.registerChannel:
					call(worker, "Worker.DoJob", &jobArgs, &reply)
					if (reply.OK) {
						mapChannel <- true
						mr.idleChannel <- worker
						return
					}
				case worker = <- mr.idleChannel:
					call(worker, "Worker.DoJob", &jobArgs, &reply)
					if (reply.OK) {
						mapChannel <- true
						mr.idleChannel <- worker
						return
					}
				}
			}
		}(i)
	}

	for i := 0; i < mr.nMap; i++ {
		<- mapChannel
	}
	fmt.Println("Map is Done")

	for i := 0; i < mr.nReduce; i++ {
		go func (jobN int) {
			var worker string
			var jobArgs DoJobArgs
			var reply DoJobReply
			jobArgs.File = mr.file
			jobArgs.Operation = Reduce
			jobArgs.JobNumber = jobN
			jobArgs.NumOtherPhase = mr.nMap
			for {
				select {
				case worker = <- mr.registerChannel:
					call(worker, "Worker.DoJob", jobArgs, &reply)
					if (reply.OK) {
						reduceChannel <- true
						mr.idleChannel <- worker
						return
					}
				case worker = <- mr.idleChannel:
					call(worker, "Worker.DoJob", jobArgs, &reply)
					if (reply.OK) {
						reduceChannel <- true
						mr.idleChannel <- worker
						return
					}
				}
			}
		}(i)
	}

	for i := 0; i < mr.nReduce; i++ {
		<- reduceChannel
	}

	fmt.Println("Reduce is Done")

	return mr.KillWorkers()
}
