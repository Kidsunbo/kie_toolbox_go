package kflow

import "time"

type flag struct{ id int }

var ReportInEnglish = flag{id: 1} // Report error and message in english
var ReportInChinese = flag{id: 2} // Report error and message in chinese

// The maximum time to wait for an asynchronous operation to complete. It's important to note that this is not the time limit for a node's execution but rather the maximum time to wait for a specific node.
//
// For instance, consider two nodes, A and B. Node A takes 3 seconds to execute, and node B takes 2 seconds. Suppose the asynchronous time is set to 2.5 seconds.
// 	- If the engine first waits for node A, since A's execution time of 3 seconds exceeds the asynchronous time of 2.5 seconds, a timeout error will be triggered, and the engine will return this error.
// 	- If the engine first waits for node B, as B's execution time of 2 seconds is less than the asynchronous time of 2.5 seconds, node A will execute successfully. Subsequently, when the engine starts to wait for node B, one might think B should stop because its execution time exceeds the asynchronous time. However, this is not the case. When the engine starts waiting for B, there is only 1 second remaining for B to run. So, B will continue to execute, and the engine will still wait for B for a total of 2.5 seconds. As a result, B can execute successfully, and no error will be reported.
type AsyncTimeout int64 

type config struct {
	Name     string
	Language int8
	Timeout  time.Duration
}
