package pbft

type bootCommand int

const (
	halt bootCommand = iota
	boot
)
