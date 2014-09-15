/*
 Package folderscanner provides util functions for general programing
*/

package folderscanner

import (
	"fmt"
	"os"
	pathpkg "path"
	"regexp"
	"sync"
	"time"

	"github.com/wheelcomplex/goqueue"
	//	"github.com/wheelcomplex/misc"
)

// FileInfo of /dev/null
var DevNullFileInfo os.FileInfo

//
func init() {
	var err error
	//
	DevNullFileInfo, err = os.Lstat("/dev/null")
	if err != nil {
		panic("os.Lstat on /dev/null failed")
	}

}

type TargetType int

// targetType
const (
	FOLDER_SCAN_DIR_ONLY TargetType = iota // default target
	FOLDER_SCAN_FILE_ONLY
	FOLDER_SCAN_ALL
)

// PathInfo contain file/path info from scan
// Stat = DevNullFileInfo if scan error
type PathInfo struct {
	Path     string      // full path
	Stat     os.FileInfo // os.Stat info
	IsFolder bool        //
	Err      error
}

// FolderScanner scan dir and output to channel
// scanner will check include filter(if no nil) and exclude filter(if no nil)
type FolderScanner struct {
	targetType   TargetType                // FOLDER_SCAN_ALL || FOLDER_SCAN_FILE_ONLY || FOLDER_SCAN_DIR_ONLY
	recursive    bool                      // recursively scan sub-dir if true
	workers      int                       // running goroutine of scanner
	incRegex     map[string]*regexp.Regexp // compiled output include filter
	excRegex     map[string]*regexp.Regexp // compiled output exclude filter
	scanIncRegex map[string]*regexp.Regexp // compiled scanInclude filter
	scanExcRegex map[string]*regexp.Regexp // compiled scanExclude filter
	outputInfo   chan *PathInfo
	inRun        string         // scanning path, check for busy
	mu           *sync.Mutex    // folderscanner lock
	stack        *goqueue.Stack // scan queue
	jobIn        int64          // number of scanning
	jobOut       int64          // number of scan done
	jobFile      int64          // number of scanned file
	jobMu        *sync.Mutex    // folderscanner lock
	closing      chan struct{}  // require for reset
	alldone      chan struct{}  // require for reset
	exited       chan struct{}  // comfirm for reset

}

const defaultWorkers = 1

func NewFolderScanner() *FolderScanner {
	self := &FolderScanner{
		targetType:   FOLDER_SCAN_ALL,
		recursive:    false,
		workers:      defaultWorkers,
		incRegex:     make(map[string]*regexp.Regexp),
		excRegex:     make(map[string]*regexp.Regexp),
		scanIncRegex: make(map[string]*regexp.Regexp),
		scanExcRegex: make(map[string]*regexp.Regexp),
		outputInfo:   make(chan *PathInfo, defaultWorkers*2048),
		alldone:      make(chan struct{}, 1),
		closing:      make(chan struct{}, 1),
		exited:       make(chan struct{}, defaultWorkers),
		inRun:        "",
		stack:        nil,
		jobIn:        0,
		jobOut:       0,
		jobFile:      0,
		jobMu:        &sync.Mutex{},
		mu:           &sync.Mutex{},
	}
	return self
}

// Close is alais of Reset()
func (self *FolderScanner) Reset() {
	self.Close()
}

// Scan start background scan goroutine, and return output channel
// if all scan done, output channel will be closed(and read from this channel will got <nil>)
// user should check val,ok = <-out to identify scan has done
// user have call self.Reset() befor start a new scan job
func (self *FolderScanner) Scan(path string, target TargetType, recursive bool) (chan *PathInfo, error) {
	self.mu.Lock()
	defer self.mu.Unlock()
	if len(self.inRun) > 0 {
		return nil, fmt.Errorf("scanner is busy: %s", self.inRun)
	}
	if len(path) == 0 {
		return nil, fmt.Errorf("invalid path for scanner: %s", path)
	}
	// renew scanner
	self.closing = make(chan struct{}, 1)
	self.alldone = make(chan struct{}, 1)
	self.exited = make(chan struct{}, self.workers)
	self.outputInfo = make(chan *PathInfo, self.workers*2048)
	self.stack = goqueue.NewStack(512, -1, false)
	//
	self.recursive = recursive
	self.targetType = target
	pinfo, err := self.addPathInfo(0, true, path)
	if err != nil {
		return nil, fmt.Errorf("scan %s: %v", path, err)
	}
	self.inRun = path
	// path is dir and lstat ok
	for i := 0; i < self.workers; i++ {
		time.Sleep(1e5)
		go self.folderScanner(int64(i + 1))
	}
	if pinfo.IsFolder {
		//go self.wait(path, true)
	} else {
		go self.Close()
	}
	return self.outputInfo, nil
}

// Stat return workers, stack cache size, jobIn, jobOut, jobFile
func (self *FolderScanner) Stat() (int64, int64, int64, int64, int64) {
	return int64(self.workers), self.stack.GetCacheSize(), self.jobIn, self.jobOut, self.jobFile
}

// Close stop and re-initial scanner
func (self *FolderScanner) Close() {
	self.mu.Lock()
	defer self.mu.Unlock()
	select {
	case <-self.closing:
		// already closed
		return
	default:
	}
	if len(self.inRun) == 0 {
		return
	}
	// clean channels and close output
	close(self.closing)
	// waiting for all scanner goroutine exit
	for i := self.workers; i > 0; i-- {
		<-self.exited
	}
	self.stack.Close()
	// all scanner goroutine exited
	close(self.outputInfo)
	//fmt.Println(self.workers, "workers end for all done,", self.jobOut, "folders in", self.inRun, " out files", self.jobFile)
	//
	// self.workers
	self.jobOut = 0
	self.jobFile = 0
	self.jobIn = 0
	//
	self.inRun = ""
	// scanner is idle now
	return
}

//
func (self *FolderScanner) folderScanner(id int64) {
	//fmt.Println(id, "folderScanner goroutine ...")
	outCh := self.stack.Out()
	loop := true
	for loop {
		select {
		case out := <-outCh:
			//toscan := out.(string)
			//fmt.Println("folderScanner in: ", toscan, ", target", self.targetType, ", recursive", self.recursive, ", include ", self.incFilter, ", exclude ", self.excFilter)
			self.readDir(out.(string), id)
			self.jobMu.Lock()
			self.jobOut++
			if self.jobIn > 0 && self.jobOut == self.jobIn {
				// all sub-scan is done
				//fmt.Println(id, "all sub-scan done")
				go self.Close()
			}
			self.jobMu.Unlock()
		case <-self.closing:
			loop = false
		}

	}
	self.exited <- struct{}{}
	//fmt.Println(id, "folderScanner goroutine exited")
}

//
func (self *FolderScanner) readDir(path string, id int64) {
	//fmt.Println(id, "readDir", path)
	f, err := os.Open(path)
	if err != nil {
		self.outputInfo <- &PathInfo{Stat: DevNullFileInfo, Path: path, Err: err, IsFolder: false}
		return
	}
	names, err := f.Readdirnames(-1)
	f.Close()
	if err != nil {
		self.outputInfo <- &PathInfo{Stat: DevNullFileInfo, Path: path, Err: err, IsFolder: false}
		return
	}
	// stat the names
	for _, name := range names {
		newPath := pathpkg.Join(path, name)
		self.addPathInfo(id, self.recursive, newPath)
	}
	return
}

// O(n) func
func (self *FolderScanner) addPathInfo(id int64, recursive bool, newPath string) (*PathInfo, error) {
	var pinfo *PathInfo = nil
	info, err2 := os.Lstat(newPath)
	if err2 != nil {
		pinfo = &PathInfo{Stat: DevNullFileInfo, Path: newPath, Err: err2, IsFolder: false}
		self.outputInfo <- pinfo
		return pinfo, err2
	}
	if info.IsDir() {
		if self.targetType != FOLDER_SCAN_FILE_ONLY {
			pinfo = &PathInfo{Stat: info, Path: newPath, Err: nil, IsFolder: true}
			if self.outputMatch(newPath) {
				self.outputInfo <- pinfo
			}
			//fmt.Println(id, "add dir", newPath)
		} else {
			//fmt.Println(id, "skip dir by FOLDER_SCAN_FILE_ONLY", newPath)
		}
		select {
		case <-self.closing:
			// no more input for closing
			//fmt.Printf("closing, skipped %s\n", newPath)
		default:
			if recursive && self.scanMatch(newPath) {
				self.jobMu.Lock()
				self.jobIn++
				self.jobMu.Unlock()
				if pcode := self.stack.Push(newPath, true); pcode != goqueue.PUSH_OK {
					//fmt.Println(id, "push self.stack.In() failed", newPath)
					self.jobMu.Lock()
					self.jobIn--
					self.jobMu.Unlock()
					return pinfo, fmt.Errorf("push new path %s into stack failed: %d", newPath, pcode)
				}
			}
		}
	} else if self.targetType == FOLDER_SCAN_DIR_ONLY {
		//fmt.Println(id, "skip file by FOLDER_SCAN_DIR_ONLY", newPath)
	} else if info.Mode().IsRegular() {
		// include regular file
		pinfo = &PathInfo{Stat: info, Path: newPath, Err: nil, IsFolder: false}
		if self.outputMatch(newPath) {
			self.outputInfo <- pinfo
			self.jobMu.Lock()
			self.jobFile++
			self.jobMu.Unlock()
		}
	} else {
		//fmt.Println(id, "skip file by no regular file", newPath)
	}
	return pinfo, nil
}

// SetOutputFilter
// filter effect output only
// inc == true to set INCLUDE filter
// inc == false to set EXCLUDE filter
func (self *FolderScanner) SetOutputFilter(inc bool, pattern string) error {
	if inc {
		return self.setIncludeFilter(pattern)
	}
	return self.setExcludeFilter(pattern)
}

// setIncludeFilter
// filter effect output only
func (self *FolderScanner) setIncludeFilter(pattern string) error {
	self.mu.Lock()
	defer self.mu.Unlock()
	if len(self.inRun) > 0 {
		return fmt.Errorf("scanner is busy: %s", self.inRun)
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return err
	}
	self.incRegex[pattern] = re
	return nil
}

// setExcludeFilter
// filter effect output only
func (self *FolderScanner) setExcludeFilter(pattern string) error {
	self.mu.Lock()
	defer self.mu.Unlock()
	if len(self.inRun) > 0 {
		return fmt.Errorf("scanner is busy: %s", self.inRun)
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return err
	}
	self.excRegex[pattern] = re
	return nil
}

// SetScanFilter
// filter effect scanning path
// inc == true to set INCLUDE filter
// inc == false to set EXCLUDE filter
func (self *FolderScanner) SetScanFilter(inc bool, pattern string) error {
	if inc {
		return self.scanIncludeFilter(pattern)
	}
	return self.scanExcludeFilter(pattern)
}

// scanIncludeFilter
// filter effect output only
func (self *FolderScanner) scanIncludeFilter(pattern string) error {
	self.mu.Lock()
	defer self.mu.Unlock()
	if len(self.inRun) > 0 {
		return fmt.Errorf("scanner is busy: %s", self.inRun)
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return err
	}
	self.scanIncRegex[pattern] = re
	return nil
}

// scanExcludeFilter
// filter effect output only
func (self *FolderScanner) scanExcludeFilter(pattern string) error {
	self.mu.Lock()
	defer self.mu.Unlock()
	if len(self.inRun) > 0 {
		return fmt.Errorf("scanner is busy: %s", self.inRun)
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return err
	}
	self.scanExcRegex[pattern] = re
	return nil
}

// SetWorker set scanner goroutine
// will no effect if n < 3
// n = CPUs * 2 will be fastest
// return old value
func (self *FolderScanner) SetWorker(n int) (int, error) {
	old := self.workers
	if n < 1 {
		return old, nil
	}
	self.mu.Lock()
	defer self.mu.Unlock()
	if len(self.inRun) > 0 {
		return old, fmt.Errorf("scanner is busy: %s", self.inRun)
	}
	self.workers = n
	return old, nil
}

// outputMatch return false if path should not be send to output
func (self *FolderScanner) outputMatch(newPath string) bool {
	// check regexp
	var match string
	var mOk bool
	if len(self.incRegex) > 0 {
		mOk = false
		for match, _ = range self.incRegex {
			if self.incRegex[match].MatchString(newPath) == true {
				//fmt.Println("include", match, "match", newPath)
				mOk = true
				break
			}
		}
		if mOk == false {
			return false
		}
	}
	if len(self.excRegex) > 0 {
		mOk = false
		for match, _ = range self.excRegex {
			if self.excRegex[match].MatchString(newPath) == true {
				mOk = true
				//fmt.Println("exclude", match, "match", newPath)
				break
			}
		}
		if mOk == true {
			return false
		}
	}
	return true
}

// scanMatch return false if path should not be send to recursive scan
func (self *FolderScanner) scanMatch(newPath string) bool {
	// check regexp
	var match string
	var mOk bool
	if len(self.scanIncRegex) > 0 {
		mOk = false
		for match, _ = range self.scanIncRegex {
			if self.scanIncRegex[match].MatchString(newPath) == true {
				//fmt.Println("scanInclude", match, "match", newPath)
				mOk = true
				break
			}
		}
		if mOk == false {
			return false
		}
	}
	if len(self.scanExcRegex) > 0 {
		mOk = false
		for match, _ = range self.scanExcRegex {
			if self.scanExcRegex[match].MatchString(newPath) == true {
				mOk = true
				//fmt.Println("scanExclude", match, "match", newPath)
				break
			}
		}
		if mOk == true {
			return false
		}
	}
	return true
}

//
