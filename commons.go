package main

const (
	MASTER = 0
	WORKER = 1
	REGISTER_WORKER = 2
	SOLUTION_WORKER = 3
)

type Data struct {
	Id int `json:"id"`
	Dimensions  []float64 `json:"Dimensions"`
	Cluster int
}

