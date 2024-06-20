package queue_test

import (
	"testing"

	"github.com/lanrat/extsort/queue"
)

func intCompareFunc(a interface{}, b interface{}) bool {
	ai := a.(int)
	bi := b.(int)
	return ai < bi
}

func TestInit0(t *testing.T) {
	q := queue.NewPriorityQueue(intCompareFunc)
	for i := 20; i > 0; i-- {
		q.Push(0) // all elements are the same
	}

	l := q.Len()
	if l != 20 {
		t.Fatalf("queue len is %d, expected %d", l, 20)
	}

	for i := 1; q.Len() > 0; i++ {
		x := q.Peek().(int)
		y := q.Pop().(int)
		if x != y {
			t.Fatalf("q.Peek() and q.Pop() returned different values %d %d", x, y)
		}
		if x != 0 {
			t.Errorf("%d.th pop got %d; want %d", i, x, 0)
		}
	}
}

func Test(t *testing.T) {
	q := queue.NewPriorityQueue(intCompareFunc)
	l := q.Len()
	if l != 0 {
		t.Fatalf("queue len is %d, expected %d", l, 0)
	}

	for i := 20; i > 10; i-- {
		q.Push(i)
	}

	l = q.Len()
	if l != 10 {
		t.Fatalf("queue len is %d, expected %d", l, 10)
	}

	for i := 10; i > 0; i-- {
		q.Push(i)
	}

	l = q.Len()
	if l != 20 {
		t.Fatalf("queue len is %d, expected %d", l, 20)
	}

	for i := 1; q.Len() > 0; i++ {
		x := q.Peek().(int)
		y := q.Pop().(int)
		if x != y {
			t.Fatalf("q.Peek() and q.Pop() returned different values %d %d", x, y)
		}
		if i < 20 {
			q.Push(20 + i)
		}
		if x != i {
			t.Errorf("%d.th pop got %d; want %d", i, x, i)
		}
	}
}
