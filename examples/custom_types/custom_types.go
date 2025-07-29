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

func personToBytes(p Person) []byte {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(p)
	if err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func personFromBytes(data []byte) Person {
	var p Person
	buf := bytes.NewReader(data)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&p)
	if err != nil {
		panic(err)
	}
	return p
}

func comparePersons(a, b Person) bool {
	return a.Age < b.Age // Sort by age
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
		comparePersons,
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
