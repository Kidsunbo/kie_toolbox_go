package logs

import (
	"context"
	"fmt"
	"os"
	"testing"
)

func TestLogsWithoutContext(t *testing.T) {
	defer func() {
		if a := recover(); a != nil {
			fmt.Println("[PASS]")
			return
		}
		t.Fail()
	}()
	Debug("hello %v, and this is %v with age %v", "world", "tom", 12)
	Info("hello %v, and this is %v with age %v", "world", "tom", 12)
	Warn("hello %v, and this is %v with age %v", "world", "tom", 12)
	Error("hello %v, and this is %v with age %v", "world", "tom", 12)
	Fatal("hello %v, and this is %v with age %v", "world", "tom", 12)
}

func TestLogsWithContext(t *testing.T) {
	ctx := context.Background()
	defer func() {
		if a := recover(); a != nil {
			fmt.Println("[PASS]")
			return
		}
		t.Fail()
	}()
	CtxDebug(ctx, "hello %v, and this is %v with age %v", "world", "tom", 12)
	CtxInfo(ctx, "hello %v, and this is %v with age %v", "world", "tom", 12)
	CtxWarn(ctx, "hello %v, and this is %v with age %v", "world", "tom", 12)
	CtxError(ctx, "hello %v, and this is %v with age %v", "world", "tom", 12)
	CtxFatal(ctx, "hello %v, and this is %v with age %v", "world", "tom", 12)
}

func TestLogsWithContextLogID(t *testing.T) {
	ctx := context.Background()
	ctx = context.WithValue(ctx, "X-Request-ID", "1234567890")
	ctx = context.WithValue(ctx, "X-Real-IP", "187.0.55.67")
	defer func() {
		if a := recover(); a != nil {
			fmt.Println("[PASS]")
			return
		}
		t.Fail()
	}()
	CtxDebug(ctx, "hello %v, and this is %v with age %v", "world", "tom", 12)
	CtxInfo(ctx, "hello %v, and this is %v with age %v", "world", "tom", 12)
	CtxWarn(ctx, "hello %v, and this is %v with age %v", "world", "tom", 12)
	CtxError(ctx, "hello %v, and this is %v with age %v", "world", "tom", 12)
	CtxFatal(ctx, "hello %v, and this is %v with age %v", "world", "tom", 12)
}

func TestLogsSetOutputFile(t *testing.T) {
	ctx := context.Background()
	ctx = context.WithValue(ctx, "X-Request-ID", "1234567890")
	defer func() {
		err := os.Remove("hello.log")
		if err != nil {
			fmt.Println("[NOTICE] Fail to delete file", err)
		}
		if a := recover(); a != nil {
			fmt.Println("[PASS]")
			return
		}
		t.Fail()
	}()
	Default().SetOutputFile("hello.log")
	CtxDebug(ctx, "hello %v, and this is %v with age %v", "world", "tom", 12)
	CtxInfo(ctx, "hello %v, and this is %v with age %v", "world", "tom", 12)
	CtxWarn(ctx, "hello %v, and this is %v with age %v", "world", "tom", 12)
	CtxError(ctx, "hello %v, and this is %v with age %v", "world", "tom", 12)

	Default().SetOutputFile("")
	CtxDebug(ctx, "hello %v, and this is %v with age %v", "world", "tom", 12)
	CtxInfo(ctx, "hello %v, and this is %v with age %v", "world", "tom", 12)
	CtxWarn(ctx, "hello %v, and this is %v with age %v", "world", "tom", 12)
	CtxError(ctx, "hello %v, and this is %v with age %v", "world", "tom", 12)
	CtxFatal(ctx, "hello %v, and this is %v with age %v", "world", "tom", 12)
}

func TestLogsSetLevel(t *testing.T) {
	ctx := context.Background()
	ctx = context.WithValue(ctx, "X-Request-ID", "1234567890")
	defer func() {
		if a := recover(); a != nil {
			fmt.Println("[PASS]")
			return
		}
		t.Fail()
	}()
	Default().SetLevel(LevelDebug)
	Default().SetPathLength(3)
	CtxDebug(ctx, "hello %v, and this is %v with age %v", "world", "tom", 12)
	CtxInfo(ctx, "hello %v, and this is %v with age %v", "world", "tom", 12)
	CtxWarn(ctx, "hello %v, and this is %v with age %v", "world", "tom", 12)
	CtxError(ctx, "hello %v, and this is %v with age %v", "world", "tom", 12)
	CtxFatal(ctx, "hello %v, and this is %v with age %v", "world", "tom", 12)
}
