package fti_test

import (
	"github.com/AsoSunag/Fractal-Tree-Index-Go"
	"testing"
)

func TestFTInsert(t *testing.T) {
	tree := &fti.Tree{}
	tree.Init(3)
	tree.Insert("A", []byte("A"))
	tree.Insert("B", []byte("B"))
	tree.Insert("C", []byte("C"))
	tree.Insert("D", []byte("D"))
	tree.Insert("E", []byte("E"))
	tree.Insert("F", []byte("F"))
	tree.Insert("G", []byte("G"))
	tree.Print()
}
