package fti_test

import (
	"github.com/AsoSunag/Fractal-Tree-Index-Go"
	"log"
	"testing"
	"time"
)

func TestFTInsert(t *testing.T) {
	tree := &fti.Tree{}
	tree.Init()
	tree.Insert("A", []byte("A"))
	time.Sleep(1000 * 1000)
	tree.Insert("B", []byte("B"))
	time.Sleep(1000 * 1000)
	tree.Insert("C", []byte("C"))
	time.Sleep(1000 * 1000)
	val, _ := tree.Get("A")
	log.Println(string(val))
	val, _ = tree.Get("B")
	log.Println(string(val))
	val, _ = tree.Get("C")
	log.Println(string(val))
	tree.Print()
}
