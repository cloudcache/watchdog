package watchdog

import (
	"context"
	"testing"
)

func TestBackendRuntimeBackgroundServicesDisabled(t *testing.T) {
	runtime := &BackendRuntime{}
	if err := runtime.StartBackground(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := runtime.StartBackground(context.Background()); err == nil {
		t.Fatal("expected duplicate background start to fail")
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestBackendRuntimeCannotStartAfterClose(t *testing.T) {
	runtime := &BackendRuntime{}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	if err := runtime.StartBackground(context.Background()); err == nil {
		t.Fatal("expected closed backend runtime start to fail")
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
}
