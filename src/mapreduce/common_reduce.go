package mapreduce

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//
	var kv []KeyValue
	for m := 0; m < nMap; m++ {
		name := reduceName(jobName, m, reduceTask)
		file, fileerr := os.Open(name)
		if fileerr != nil {
			fmt.Println(fileerr)
		}
		dec := json.NewDecoder(file)
		var err error
		var tmp KeyValue
		err = dec.Decode(&tmp)
		for err == nil {
			kv = append(kv, tmp)
			err = dec.Decode(&tmp)
		}
		file.Close()
	}
	sort.Slice(kv, func(i, j int) bool {
		return kv[i].Key < kv[j].Key
	})
	keyLast := kv[0].Key
	var values [][]string
	var keys []string
	idx := 0
	keys = append(keys, kv[0].Key)
	values = append(values, make([]string, 0))
	for _, val := range kv {
		if keyLast != val.Key {
			idx++
			keys = append(keys, val.Key)
			keyLast = val.Key
			values = append(values, make([]string, 0))
		}
		values[idx] = append(values[idx], val.Value)
	}
	// Output
	outputfile, _ := os.OpenFile(outFile, os.O_CREATE|os.O_WRONLY, 0600)
	enc := json.NewEncoder(outputfile)
	for i, key := range keys {
		enc.Encode(KeyValue{key, reduceF(key, values[i])})
	}
	outputfile.Close()
}
