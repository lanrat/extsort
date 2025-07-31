package main

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"

	"github.com/lanrat/extsort"
)

type Person struct {
	Name string
	Age  int
}

func personToBytes(p Person) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(p)
	return buf.Bytes(), err
}

func personFromBytes(data []byte) (Person, error) {
	var p Person
	buf := bytes.NewReader(data)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&p)
	return p, err
}

func comparePersonsByAge(a, b Person) int {
	// Sort by age
	if a.Age != b.Age {
		if a.Age < b.Age {
			return -1
		}
		return 1
	}
	return 0
}

func main() {
	people := []Person{
		{"Alice", 30},
		{"Bob", 25},
		{"Charlie", 35},
	}

	inputChan := make(chan Person, len(people))
	for _, person := range people {
		inputChan <- person
	}
	close(inputChan)

	sorter, outputChan, errChan := extsort.Generic(
		inputChan,
		personFromBytes,
		personToBytes,
		comparePersonsByAge,
		nil,
	)

	go sorter.Sort(context.Background())

	fmt.Println("People sorted by age:")
	for person := range outputChan {
		fmt.Printf("%s (age %d)\n", person.Name, person.Age)
	}

	if err := <-errChan; err != nil {
		panic(err)
	}
}
