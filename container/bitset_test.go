package container

import (
	"testing"
)

func TestAtomicBitSet_SetGet(t *testing.T) {
	bs := NewAtomicBitSet(130)
	bs.Set(0)
	bs.Set(64)
	bs.Set(129)

	if !bs.Get(0) {
		t.Errorf("bit 0 should be set")
	}
	if !bs.Get(64) {
		t.Errorf("bit 64 should be set")
	}
	if !bs.Get(129) {
		t.Errorf("bit 129 should be set")
	}
	if bs.Get(1) {
		t.Errorf("bit 1 should not be set")
	}
	if bs.Get(128) {
		t.Errorf("bit 128 should not be set")
	}
}

func TestAtomicBitSet_Clear(t *testing.T) {
	bs := NewAtomicBitSet(70)
	bs.Set(10)
	bs.Set(69)
	bs.Clear(10)
	bs.Clear(69)

	if bs.Get(10) {
		t.Errorf("bit 10 should be cleared")
	}
	if bs.Get(69) {
		t.Errorf("bit 69 should be cleared")
	}
}

func TestAtomicBitSet_Any(t *testing.T) {
	bs := NewAtomicBitSet(100)
	bs.Set(10)
	bs.Set(50)
	bs.Set(99)

	if !bs.Any(10, 11) {
		t.Errorf("Any should be true for set bit")
	}
	if bs.Any(11, 20) {
		t.Errorf("Any should be false for unset range")
	}
	if !bs.Any(0, 51) {
		t.Errorf("Any should be true for range including set bits")
	}
	if !bs.Any(90, 100) {
		t.Errorf("Any should be true for range including bit 99")
	}
	if !bs.Any(1, 128) {
		t.Errorf("Any should be true for range including bit 10, 50, 99")
	}
}

func TestAtomicBitSet_All(t *testing.T) {
	bs := NewAtomicBitSet(256)
	for i := range 256 {
		bs.Set(i)
	}
	if !bs.All(0, 10) {
		t.Errorf("All should be true when all bits are set")
	}
	if !bs.All(0, 256) {
		t.Errorf("All should be true for range where all bits are set")
	}
	bs.Clear(5)
	if bs.All(0, 10) {
		t.Errorf("All should be false when a bit is cleared")
	}
	if !bs.All(0, 5) {
		t.Errorf("All should be true for range where all bits are set")
	}
	if bs.All(0, 256) {
		t.Errorf("All should be false for range where all bits are set")
	}
}

func TestAtomicBitSet_PanicsSet(t *testing.T) {
	bs := NewAtomicBitSet(10)
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic for out of range bit")
		}
	}()
	bs.Set(100)
}

func TestAtomicBitSet_PanicsSet1(t *testing.T) {
	bs := NewAtomicBitSet(10)
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic for out of range bit")
		}
	}()
	bs.Set(-1)
}

func TestAtomicBitSet_PanicsGet(t *testing.T) {
	bs := NewAtomicBitSet(10)
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic for out of range bit")
		}
	}()
	bs.Get(100)
}

func TestAtomicBitSet_PanicsGet1(t *testing.T) {
	bs := NewAtomicBitSet(10)
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic for out of range bit")
		}
	}()
	bs.Get(-1)
}

func TestAtomicBitSet_PanicsAll(t *testing.T) {
	bs := NewAtomicBitSet(10)
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic for out of range bit")
		}
	}()
	bs.All(-1, 5)
}

func TestAtomicBitSet_PanicsAll2(t *testing.T) {
	bs := NewAtomicBitSet(10)
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic for out of range bit")
		}
	}()
	bs.All(5, 100)
}

func TestAtomicBitSet_PanicsAll3(t *testing.T) {
	bs := NewAtomicBitSet(10)
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic for out of range bit")
		}
	}()
	bs.All(5, 1)
}

func TestAtomicBitSet_PanicsAny(t *testing.T) {
	bs := NewAtomicBitSet(10)
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic for out of range bit")
		}
	}()
	bs.Any(-1, 5)
}

func TestAtomicBitSet_PanicsAny2(t *testing.T) {
	bs := NewAtomicBitSet(10)
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic for out of range bit")
		}
	}()
	bs.Any(5, 100)
}

func TestAtomicBitSet_PanicsAny3(t *testing.T) {
	bs := NewAtomicBitSet(10)
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic for out of range bit")
		}
	}()
	bs.Any(5, 1)
}
