package kflow


type Flag struct{ id int }

var StaticNodes = Flag{id:1} // The nodes will never changed after initialization