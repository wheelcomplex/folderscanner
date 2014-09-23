/*
 Package folderscanner provides util functions for general programing
*/

package folderscanner

import (
	"fmt"
	"os"
	//	pathpkg "path"
	"regexp"
	"sync"
	"time"

	"github.com/wheelcomplex/goqueue/stack"
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
	outputInfo   chan<- interface{}
	inRun        string        // scanning path, check for busy
	mu           *sync.Mutex   // folderscanner lock
	inStack      *stack.Stack  // scan queue
	outStack     *stack.Stack  // output queue
	jobIn        uint64        // number of scanning
	jobOut       uint64        // number of scan done
	jobFile      uint64        // number of scanned file
	jobMu        *sync.Mutex   // folderscanner lock
	closing      chan struct{} // require for reset
	alldone      chan struct{} // require for reset
	exited       chan struct{} // comfirm for reset

}

func NewFolderScanner(num int) *FolderScanner {
	if num < 1 {
		num = 1
	}
	self := &FolderScanner{
		targetType:   FOLDER_SCAN_ALL,
		recursive:    false,
		workers:      num,
		incRegex:     make(map[string]*regexp.Regexp),
		excRegex:     make(map[string]*regexp.Regexp),
		scanIncRegex: make(map[string]*regexp.Regexp),
		scanExcRegex: make(map[string]*regexp.Regexp),
		outputInfo:   nil,
		alldone:      make(chan struct{}, 1),
		closing:      make(chan struct{}, 1),
		exited:       make(chan struct{}, num+1),
		inRun:        "",
		inStack:      nil,
		outStack:     nil,
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
func (self *FolderScanner) Scan(path string, target TargetType, recursive bool) (<-chan interface{}, error) {
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
	self.exited = make(chan struct{}, self.workers+1)
	self.inStack = stack.NewStack(512, -1, false)
	self.outStack = stack.NewStack(512, -1, false)
	self.outputInfo = self.outStack.In()
	//
	self.recursive = recursive
	self.targetType = target
	// root path always scanned
	pathStat, err := os.Lstat(path)
	//fmt.Println("root path", path, pathStat, err)
	if err != nil {
		return nil, fmt.Errorf("scan %s: %v", path, err)
	}
	self.inRun = path
	//
	for i := 0; i < self.workers; i++ {
		time.Sleep(1e5)
		go self.folderScanner(int64(i + 1))
	}
	if pathStat.IsDir() {
		// root path always scanned
		self.jobMu.Lock()
		self.jobIn++
		self.jobMu.Unlock()
		if pcode := self.inStack.Push(path, true); pcode != stack.PUSH_OK {
			fmt.Println("push root path into self.inStack.In() failed", path, pcode)
			self.jobMu.Lock()
			self.jobIn--
			self.jobMu.Unlock()
			return nil, fmt.Errorf("push new path %s into stack failed: %d", path, pcode)
		}
		//
	} else {
		go self.Close()
	}
	return self.outStack.Out(), nil
}

// Stat return workers, inStack cache size, outStack cache size, jobIn, jobOut, jobFile
func (self *FolderScanner) Stat() (uint64, uint64, uint64, uint64, uint64, uint64) {
	return uint64(self.workers), self.inStack.GetCacheSize(), self.outStack.GetCacheSize(), self.jobIn, self.jobOut, self.jobFile
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
	//fmt.Printf("waiting for %d scanner goroutine exit ...\n", self.workers)
	for i := 0; i < self.workers; i++ {
		<-self.exited
	}
	//fmt.Printf("waiting for %d scanner goroutine exited.\n", self.workers)
	self.inStack.Close()
	self.outStack.Close()
	// all scanner goroutine exited
	//fmt.Println(self.workers, "workers end for all done,", self.jobOut, "folders in", self.inRun, " out files", self.jobFile)
	//
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
	outCh := self.inStack.Out()
	defer func() {
		//fmt.Println(id, "folderScanner goroutine exiting ...")
		self.exited <- struct{}{}
		//fmt.Println(id, "folderScanner goroutine exited")
	}()
	for {
		select {
		case out := <-outCh:
			//toscan := out.(string)
			self.readDir(out.(string), id)
			self.jobMu.Lock()
			self.jobOut++
			if self.jobIn > 0 && self.jobOut == self.jobIn {
				// all sub-scan is done
				//fmt.Println(id, "all sub-scan done")
				go self.Close()
				// will exit by closing
				self.jobMu.Unlock()
				return
			}
			self.jobMu.Unlock()
		case <-self.closing:
			return
		}

	}
	return
}

//
func (self *FolderScanner) readDir(path string, id int64) {
	//fmt.Println(id, "readDir", path)
	//defer fmt.Println(id, "readDir", path, "done")
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
		newPath := path + "/" + name
		select {
		case <-self.closing:
			// no more input for closing
			//fmt.Printf("closing, readDir skipped %s\n", newPath)
			return
		default:
		}
		self.addPathInfo(id, self.recursive, newPath)
	}
	return
}

// O(n) func
func (self *FolderScanner) addPathInfo(id int64, recursive bool, newPath string) (*PathInfo, error) {
	//fmt.Println(id, "addPathInfo", newPath)
	//defer fmt.Println(id, "addPathInfo", newPath, "done")
	var pathInfo *PathInfo
	info, err2 := os.Lstat(newPath)
	//fmt.Println("enter self.addPathInfo(0, true, path):", newPath, info, err2)
	if err2 != nil {
		pathInfo = &PathInfo{Stat: DevNullFileInfo, Path: newPath, Err: err2, IsFolder: false}
		self.outputInfo <- pathInfo
		return pathInfo, err2
	}
	if info.IsDir() {
		if self.targetType != FOLDER_SCAN_FILE_ONLY {
			if self.outputMatch(newPath) {
				pathInfo = &PathInfo{Stat: info, Path: newPath, Err: nil, IsFolder: true}
				self.outputInfo <- pathInfo
			}
			//fmt.Println(id, "add dir", newPath)
		} else {
			//fmt.Println(id, "skip dir by FOLDER_SCAN_FILE_ONLY", newPath)
		}
		select {
		case <-self.closing:
			// no more input for closing
			perr := fmt.Errorf("closing, addPathInfo skipped %s\n", newPath)
			pathInfo = &PathInfo{Stat: info, Path: newPath, Err: perr, IsFolder: true}
			return pathInfo, perr
		default:
			if recursive && self.scanMatch(newPath) {
				self.jobMu.Lock()
				self.jobIn++
				self.jobMu.Unlock()
				//fmt.Println(id, "push self.inStack.In()", newPath)
				//if pcode := self.inStack.Push(newPath, false); pcode != stack.PUSH_OK {
				//	fmt.Println(id, "non-blocking push self.inStack.In() failed", newPath, pcode)
				if pcode := self.inStack.Push(newPath, true); pcode != stack.PUSH_OK {
					fmt.Println(id, "push self.inStack.In() failed", newPath, pcode)
					self.jobMu.Lock()
					self.jobIn--
					self.jobMu.Unlock()
					perr := fmt.Errorf("push new path %s into stack failed: %d", newPath, pcode)
					pathInfo = &PathInfo{Stat: info, Path: newPath, Err: perr, IsFolder: true}
					self.outputInfo <- pathInfo
					return pathInfo, perr
				}
				//}
				//fmt.Println(id, "push self.inStack.In()", newPath, "done")
				pathInfo = &PathInfo{Stat: info, Path: newPath, Err: nil, IsFolder: true}
				return pathInfo, nil
			}
		}
	} else if self.targetType == FOLDER_SCAN_DIR_ONLY {
		//fmt.Println(id, "skip file by FOLDER_SCAN_DIR_ONLY", newPath)
	} else if info.Mode().IsRegular() {
		// include regular file
		if self.outputMatch(newPath) {
			pathInfo = &PathInfo{Stat: info, Path: newPath, Err: nil, IsFolder: false}
			self.outputInfo <- pathInfo
			self.jobMu.Lock()
			self.jobFile++
			self.jobMu.Unlock()
			return pathInfo, nil
		}
	} else {
		fmt.Println(id, "skip file by no regular file", newPath)
	}
	return nil, nil
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

// DelOutputFilter
// filter effect output only
// inc == true to del INCLUDE filter
// inc == false to del EXCLUDE filter
func (self *FolderScanner) DelOutputFilter(inc bool, pattern string) error {
	self.mu.Lock()
	defer self.mu.Unlock()
	if len(self.inRun) > 0 {
		return fmt.Errorf("scanner is busy: %s", self.inRun)
	}
	if inc {
		if _, ok := self.incRegex[pattern]; ok {
			delete(self.incRegex, pattern)
		}
	} else {
		if _, ok := self.excRegex[pattern]; ok {
			delete(self.excRegex, pattern)
		}
	}
	return nil
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

// DelScanFilter
// filter effect scanning path
// inc == true to del INCLUDE filter
// inc == false to del EXCLUDE filter
func (self *FolderScanner) DelScanFilter(inc bool, pattern string) error {
	self.mu.Lock()
	defer self.mu.Unlock()
	if len(self.inRun) > 0 {
		return fmt.Errorf("scanner is busy: %s", self.inRun)
	}
	if inc {
		if _, ok := self.scanIncRegex[pattern]; ok {
			delete(self.scanIncRegex, pattern)
		}
	} else {
		if _, ok := self.scanExcRegex[pattern]; ok {
			delete(self.scanExcRegex, pattern)
		}
	}
	return nil
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

// ScanDir read/stat dir and return dir/file list and errors
// recursive scan supported
// will NOT include arg path in returned maps
func ScanDir(path string, target TargetType, recursive bool) (map[string]os.FileInfo, map[string]os.FileInfo, map[string]error) {
	var err error
	dirs := make(map[string]os.FileInfo)
	files := make(map[string]os.FileInfo)
	errs := make(map[string]error)
	pathStat, err := os.Lstat(path)
	//fmt.Println("ScanDir", recursive, path, pathStat, err)
	if err != nil {
		//errs[path] = fmt.Errorf("lstat %s: %v", path, err)
		return dirs, files, errs
	}
	//if pathStat.IsDir() && target != FOLDER_SCAN_FILE_ONLY {
	//	dirs[path] = pathStat
	//} else if target != FOLDER_SCAN_DIR_ONLY {
	//	files[path] = pathStat
	//}
	if pathStat.IsDir() == false {
		return dirs, files, errs
	}
	// read dir
	//fmt.Println("readDir", path)
	//defer fmt.Println(id, "readDir", path, "done")
	f, err := os.Open(path)
	if err != nil {
		errs[path] = fmt.Errorf("Open %s: %v", path, err)
		return dirs, files, errs
	}
	names, err := f.Readdirnames(-1)
	f.Close()
	if err != nil {
		errs[path] = fmt.Errorf("Readdirnames %s: %v", path, err)
		return dirs, files, errs
	}
	// stat the names
	for _, name := range names {
		newPath := path + "/" + name
		if recursive {
			nd, nf, ne := ScanDir(newPath, target, recursive)
			for key, _ := range nd {
				dirs[key] = nd[key]
				files[key] = nf[key]
				errs[key] = ne[key]
			}
		} else {
			pathStat, err := os.Lstat(newPath)
			//fmt.Println("Readdirnames", recursive, newPath, pathStat, err)
			if err != nil {
				errs[newPath] = fmt.Errorf("lstat %s: %v", newPath, err)
			} else {
				if pathStat.IsDir() && target != FOLDER_SCAN_FILE_ONLY {
					dirs[newPath] = pathStat
				} else if target != FOLDER_SCAN_DIR_ONLY {
					files[newPath] = pathStat
				}
			}
		}
	}
	return dirs, files, errs
}

//
