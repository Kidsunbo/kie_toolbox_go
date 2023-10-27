package logs

import (
	"context"
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"
	"time"
)

type Level int8

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
	LevelFatal
)

type Logger struct {
	debugLogger *log.Logger
	infoLogger  *log.Logger
	warnLogger  *log.Logger
	errorLogger *log.Logger
	fatalLogger *log.Logger
	level       Level
	ctxFunc     func(ctx context.Context) string
	pathLength  int8
	callerDepth int
	msgPrefix   string
}

func (l *Logger) SetLevel(level Level) {
	l.level = level
}

func (l *Logger) SetOutputFile(filename string) {
	if filename == "" {
		l.debugLogger.SetOutput(os.Stderr)
		l.infoLogger.SetOutput(os.Stderr)
		l.warnLogger.SetOutput(os.Stderr)
		l.errorLogger.SetOutput(os.Stderr)
		l.fatalLogger.SetOutput(os.Stderr)
	} else {
		file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			log.Fatal(err)
		}
		l.debugLogger.SetOutput(file)
		l.infoLogger.SetOutput(file)
		l.warnLogger.SetOutput(file)
		l.errorLogger.SetOutput(file)
		l.fatalLogger.SetOutput(file)
	}
}

func (l *Logger) SetCallerDepth(depth int) {
	l.callerDepth = depth
}

func (l *Logger) SetPathLength(length int8) {
	l.pathLength = length
}

func (l *Logger) SetMessagePrefix(prefix string) {
	l.msgPrefix = prefix
}

func (l *Logger) getLogMetaInfo() string {
	pc, file, line, ok := runtime.Caller(l.callerDepth)
	if !ok {
		return ""
	}

	currentDatetime := l.getCurrentDatetime()
	currentFilePosition := l.getFilePosition(file, line)
	currentFunctionName := l.getCurrentFunctionName(pc)

	return fmt.Sprintf("%v %v [%v]", currentDatetime, currentFilePosition, currentFunctionName)
}

func (l *Logger) getCurrentFunctionName(pc uintptr) string {
	functionName := runtime.FuncForPC(pc).Name()
	idx := strings.LastIndex(functionName, ".")
	if idx == -1 {
		return functionName
	} else if idx == len(functionName) {
		return functionName
	}
	return functionName[idx+1:]
}

func (l *Logger) getCurrentDatetime() string {
	return time.Now().Format("2006-01-02 15:04:05.000")
}

func (l *Logger) getFilePosition(file string, line int) string {
	idx := len(file) - 1
	var count int8
	for ; idx >= 0; idx-- {
		if file[idx] == '/' {
			count++
		}
		if count == l.pathLength {
			break
		}
	}
	var filename string
	if idx == -1 {
		filename = file
	} else {
		filename = file[idx+1:]
	}
	return fmt.Sprintf("%v:%v", filename, line)
}

func (l *Logger) SetContextFunction(f func(ctx context.Context) string) {
	l.ctxFunc = f
}

func (l *Logger) Debug(format string, values ...interface{}) {
	if l.level > LevelDebug {
		return
	}
	l.debugLogger.Println(stringify(l.getLogMetaInfo(), fmt.Sprintf(l.msgPrefix+format, values...)))
}

func (l *Logger) CtxDebug(ctx context.Context, format string, values ...interface{}) {
	if l.level > LevelDebug {
		return
	}
	var ctxStr string
	if l.ctxFunc != nil {
		ctxStr = l.ctxFunc(ctx)
	}
	l.debugLogger.Println(stringify(l.getLogMetaInfo(), ctxStr, fmt.Sprintf(l.msgPrefix+format, values...)))
}

func (l *Logger) Info(format string, values ...interface{}) {
	if l.level > LevelInfo {
		return
	}
	l.infoLogger.Println(stringify(l.getLogMetaInfo(), fmt.Sprintf(l.msgPrefix+format, values...)))
}

func (l *Logger) CtxInfo(ctx context.Context, format string, values ...interface{}) {
	if l.level > LevelInfo {
		return
	}
	var ctxStr string
	if l.ctxFunc != nil {
		ctxStr = l.ctxFunc(ctx)
	}
	l.infoLogger.Println(stringify(l.getLogMetaInfo(), ctxStr, fmt.Sprintf(l.msgPrefix+format, values...)))
}

func (l *Logger) Warn(format string, values ...interface{}) {
	if l.level > LevelWarn {
		return
	}
	l.warnLogger.Println(stringify(l.getLogMetaInfo(), fmt.Sprintf(l.msgPrefix+format, values...)))
}

func (l *Logger) CtxWarn(ctx context.Context, format string, values ...interface{}) {
	if l.level > LevelWarn {
		return
	}
	var ctxStr string
	if l.ctxFunc != nil {
		ctxStr = l.ctxFunc(ctx)
	}
	l.warnLogger.Println(stringify(l.getLogMetaInfo(), ctxStr, fmt.Sprintf(l.msgPrefix+format, values...)))
}

func (l *Logger) Error(format string, values ...interface{}) {
	if l.level > LevelError {
		return
	}
	l.errorLogger.Println(stringify(l.getLogMetaInfo(), fmt.Sprintf(l.msgPrefix+format, values...)))
}

func (l *Logger) CtxError(ctx context.Context, format string, values ...interface{}) {
	if l.level > LevelError {
		return
	}
	var ctxStr string
	if l.ctxFunc != nil {
		ctxStr = l.ctxFunc(ctx)
	}
	l.errorLogger.Println(stringify(l.getLogMetaInfo(), ctxStr, fmt.Sprintf(l.msgPrefix+format, values...)))
}

func (l *Logger) Fatal(format string, values ...interface{}) {
	if l.level > LevelFatal {
		return
	}
	l.fatalLogger.Println(stringify(l.getLogMetaInfo(), fmt.Sprintf(l.msgPrefix+format, values...)))
	panic("panic happened because fatal is reported")
}

func (l *Logger) CtxFatal(ctx context.Context, format string, values ...interface{}) {
	if l.level > LevelFatal {
		return
	}
	var ctxStr string
	if l.ctxFunc != nil {
		ctxStr = l.ctxFunc(ctx)
	}
	l.fatalLogger.Println(stringify(l.getLogMetaInfo(), ctxStr, fmt.Sprintf(l.msgPrefix+format, values...)))
	panic("panic happened because fatal is reported")
}

var logger *Logger

func init() {
	logger = New()
	logger.SetCallerDepth(3)
}

func defaultContextFunction(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	result := make([]string, 0, 2)
	if ctx.Value("X-Request-ID") != nil {
		requestID, ok := ctx.Value("X-Request-ID").(string)
		if ok {
			result = append(result, requestID)
		}
	}

	return strings.Join(result, " ")
}

func New() *Logger {
	return &Logger{
		debugLogger: log.New(os.Stderr, "[DEBUG] ", 0),
		infoLogger:  log.New(os.Stderr, "[INFO] ", 0),
		warnLogger:  log.New(os.Stderr, "[WARN] ", 0),
		errorLogger: log.New(os.Stderr, "[ERROR] ", 0),
		fatalLogger: log.New(os.Stderr, "[FATAL] ", 0),
		level:       LevelInfo,
		ctxFunc:     defaultContextFunction,
		pathLength:  1,
		callerDepth: 2,
		msgPrefix:   "",
	}
}

func Default() *Logger {
	return logger
}

func Debug(format string, values ...interface{}) {
	Default().Debug(format, values...)
}

func CtxDebug(ctx context.Context, format string, values ...interface{}) {
	Default().CtxDebug(ctx, format, values...)
}

func Info(format string, values ...interface{}) {
	Default().Info(format, values...)
}

func CtxInfo(ctx context.Context, format string, values ...interface{}) {
	Default().CtxInfo(ctx, format, values...)
}

func Warn(format string, values ...interface{}) {
	Default().Warn(format, values...)
}

func CtxWarn(ctx context.Context, format string, values ...interface{}) {
	Default().CtxWarn(ctx, format, values...)
}

func Error(format string, values ...interface{}) {
	Default().Error(format, values...)
}

func CtxError(ctx context.Context, format string, values ...interface{}) {
	Default().CtxError(ctx, format, values...)
}

func Fatal(format string, values ...interface{}) {
	Default().Fatal(format, values...)
}

func CtxFatal(ctx context.Context, format string, values ...interface{}) {
	Default().CtxFatal(ctx, format, values...)
}

func stringify(ss ...string) string {
	nonEmpty := make([]string, 0, len(ss))
	for _, s := range ss {
		if s == "" {
			continue
		}
		nonEmpty = append(nonEmpty, s)
	}
	return strings.Join(nonEmpty, " ")
}
