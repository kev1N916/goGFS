package common

 import (
	 "log"
	 "os"
 )
 
 // Logger is implemented by any logging system that is used for standard logs.
 type Logger interface {
	 Errorf(string, ...interface{})
	 Warningf(string, ...interface{})
	 Infof(string, ...interface{})
	 Debugf(string, ...interface{})
 }
 
 type loggingLevel int
 
 const (
	 DEBUG loggingLevel = iota
	 INFO
	 WARNING
	 ERROR
 )
 
 type DefaultLog struct {
	 *log.Logger
	 level loggingLevel
 }
 
 func DefaultLogger(level loggingLevel) *DefaultLog {
	 return &DefaultLog{Logger: log.New(os.Stderr, "gfs", log.Ldate | log.Ltime | log.Llongfile), level: level}
 }
 
 func (l *DefaultLog) Errorf(f string, v ...interface{}) {
	 if l.level <= ERROR {
		 l.Printf("ERROR: "+f, v...)
	 }
 }
 
 func (l *DefaultLog) Warningf(f string, v ...interface{}) {
	 if l.level <= WARNING {
		 l.Printf("WARNING: "+f, v...)
	 }
 }
 
 func (l *DefaultLog) Infof(f string, v ...interface{}) {
	 if l.level <= INFO {
		 l.Printf("INFO: "+f, v...)
	 }
 }
 
 func (l *DefaultLog) Debugf(f string, v ...interface{}) {
	 if l.level <= DEBUG {
		 l.Printf("DEBUG: "+f, v...)
	 }
 }
 