package logs

import (
	"context"
	"fmt"
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
	ctx = context.WithValue(ctx, "LOG_ID", "1234567890")
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
