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
	time.Sleep(1000 * 10000)
	val, _ := tree.Get("A")
	log.Println(string(val))
}
