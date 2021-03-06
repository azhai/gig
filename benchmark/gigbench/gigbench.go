package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/azhai/gig"
)

func main() {
	testSet()

}

func testSet() {
	file := "test/bench.db"
	err := gig.DeleteFile(file)
	if err != nil {
		fmt.Println(err)
	}
	var wg sync.WaitGroup

	appendd := func(i int) {
		defer wg.Done()
		k := []byte(fmt.Sprintf("%04d", i))
		err := gig.Set(file, k, k)
		if err != nil {
			fmt.Println(err)
		}
	}

	t1 := time.Now()
	for i := 0; i < 100; i++ {
		wg.Add(1)
		appendd(i)
	}
	wg.Wait()
	t2 := time.Now()

	fmt.Printf("The 100 Set took %v to run.\n", t2.Sub(t1))

	read := func(i int) {
		defer wg.Done()
		k := []byte(fmt.Sprintf("%04d", i))
		_, _ = gig.Get(file, k)
		//fmt.Println(string(res))

	}
	//_ = read
	t3 := time.Now()
	for i := 0; i < 100; i++ {
		wg.Add(1)
		read(i)
		//k := []byte(fmt.Sprintf("%04d", i))
		//_, _ = Get(file, k)
	}
	wg.Wait()
	t4 := time.Now()

	fmt.Printf("The 100 Get took %v to run.\n", t4.Sub(t3))

	//Sets
	var pairs [][]byte
	for i := 0; i < 100; i++ {
		k := []byte(fmt.Sprintf("%04d", i))
		pairs = append(pairs, k)
		pairs = append(pairs, k)
	}
	t5 := time.Now()
	gig.Sets(file, pairs)
	t6 := time.Now()
	fmt.Printf("The 100 Sets took %v to run.\n", t6.Sub(t5))

	t7 := time.Now()
	gig.Keys(file, nil, 0, 0, false)
	t8 := time.Now()
	fmt.Printf("The 100 Keys took %v to run.\n", t8.Sub(t7))

	t9 := time.Now()
	keys, _ := gig.Keys(file, nil, 0, 0, false)
	t10 := time.Now()
	fmt.Printf("The second 100 Keys took %v to run.\n", t10.Sub(t9))

	t11 := time.Now()
	_ = gig.Gets(file, keys)
	t12 := time.Now()
	fmt.Printf("The 100 Gets took %v to run.\n", t12.Sub(t11))
	gig.CloseAll()
}
