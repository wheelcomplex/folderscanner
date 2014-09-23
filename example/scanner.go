//
// folderscanner demo
//

package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"time"

	sc "github.com/wheelcomplex/folderscanner"
)

var cpuNum *int     // -c
var timeNum *int    // -t
var workerNum *int  // -w
var pathStr *string // -p
var statFlag *bool  // -v

func main() {

	cpuNum = flag.Int("c", 1, "use cpu number")
	workerNum = flag.Int("w", 1, "worker number")
	timeNum = flag.Int("t", 0, "close time")
	pathStr = flag.String("p", "/tmp", "path to scan")
	statFlag = flag.Bool("v", false, "show stat")
	flag.Parse()
	if *cpuNum < 1 {
		*cpuNum = 1
	}
	runtime.GOMAXPROCS(*cpuNum)
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
	scanner()
}

//
func scanner() {

	fmt.Printf("folderScanner(%d/%d): %v\n", *workerNum, *cpuNum, *pathStr)
	fsc := sc.NewFolderScanner()
	var dcounter int64 = 0
	var fcounter int64 = 0
	var ecounter int64 = 0
	//fsc.SetOutputFilter(true, "^/.*\\.go$")
	//fsc.SetOutputFilter(true, "^/.*\\.txt$")
	//fsc.SetOutputFilter(true, "^/.*/data$")
	//fsc.SetScanFilter(false, "^/mnt/.*")
	fsc.SetScanFilter(false, "^/proc/.*")
	fsc.SetWorker(*workerNum)
	pathchan, err := fsc.Scan(*pathStr, sc.FOLDER_SCAN_ALL, true)
	if err != nil {
		fmt.Printf("fsc.Scan: %v\n", err)
		return
	}
	if *timeNum > 0 {
		go func() {
			time.Sleep(1e9 * time.Duration(*timeNum))
			fsc.Close()
		}()
	}
	if *statFlag {
		go func() {
			tk := time.NewTicker(1e9)
			defer tk.Stop()
			for {
				<-tk.C
				fmt.Println(fsc.Stat())
			}
		}()
	}
	// read output until closed
	for newInfo := range pathchan {
		switch {
		case newInfo.Err != nil:
			ecounter++
			fmt.Printf("E#%d: %v\n", ecounter, newInfo.Err)
			//fsc.Close()
		case newInfo.IsFolder == true:
			dcounter++
			fmt.Printf("D#%d: %v\n", dcounter, newInfo.Path)
			//fmt.Printf("%v\n", newInfo.Path)
		default:
			fcounter++
			fmt.Printf("F#%d: %v\n", fcounter, newInfo.Path)
			//fmt.Printf("%v\n", newInfo.Path)
		}
	}
	fmt.Printf("folderScannerScan is Done: %v\n", *pathStr)
	fsc.Close()
	fmt.Printf("folderScannerScan, total dir %v file %v, errs %v\n", dcounter, fcounter, ecounter)
}

//
//
//
//
//
//
//
//
//
//
//
//
