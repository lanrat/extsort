package queue_test

// Regression tests for PriorityQueue allocations and PeekUpdate.

import (
	"cmp"
	"slices"
	"testing"

	"github.com/lanrat/extsort/queue"
)

// Push used to allocate twice per element (boxing it for container/heap, then storing a
// pointer to a copy) and ran a redundant heap.Fix. Push and Pop now allocate nothing
// once the queue has grown.
func TestPushPopDoNotAllocate(t *testing.T) {
	q := queue.NewPriorityQueue(cmp.Compare[int])
	for i := range 64 {
		q.Push(i * 1000)
	}
	allocs := testing.AllocsPerRun(100, func() {
		q.Push(q.Pop() + 1000)
	})
	if allocs != 0 {
		t.Errorf("Push and Pop allocated %v times per call, want 0", allocs)
	}
}

// The merge updates the top element in place and calls PeekUpdate, as tested here.
func TestPeekUpdate(t *testing.T) {
	type source struct{ next int }
	q := queue.NewPriorityQueue(func(a, b *source) int { return cmp.Compare(a.next, b.next) })
	for _, v := range []int{5, 1, 4, 2, 3} {
		q.Push(&source{v})
	}
	var got []int
	for q.Len() > 0 {
		top := q.Peek()
		got = append(got, top.next)
		if top.next < 10 {
			top.next += 10 // each source yields its value, then that value plus 10
			q.PeekUpdate()
		} else {
			q.Pop()
		}
	}
	want := []int{1, 2, 3, 4, 5, 11, 12, 13, 14, 15}
	if !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// BenchmarkPeekUpdate replaces the top of a 64-element queue, as a 64-way merge does.
func BenchmarkPeekUpdate(b *testing.B) {
	type source struct{ next int }
	q := queue.NewPriorityQueue(func(a, b *source) int { return cmp.Compare(a.next, b.next) })
	for i := range 64 {
		q.Push(&source{i})
	}
	b.ReportAllocs()
	for b.Loop() {
		s := q.Peek()
		s.next += 64
		q.PeekUpdate()
	}
}

// BenchmarkPushPop pushes and pops through a 64-element queue.
func BenchmarkPushPop(b *testing.B) {
	q := queue.NewPriorityQueue(cmp.Compare[int])
	for i := range 64 {
		q.Push(i * 1000)
	}
	b.ReportAllocs()
	for b.Loop() {
		q.Push(q.Pop() + 64000)
	}
}
