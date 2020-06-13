package main 

import (
	"strings"
	"strconv"
	"fmt"
	"bufio"
	"os"
	"math"
	"net"
	"encoding/json"
	"sync"
)

type TCPResponse struct{
	Id int `json:"id"`
	Data string `json:"data"`
}

func (t TCPResponse) toJson() string {
	json, _ := json.Marshal(t)
	return string(json)
} 



var rol int
var masterInfo MasterIp
var workerInfo WorkerIp

var partialSolutions = make(chan Solution)

var workerIps []string

type FinalSolution struct {
	Centroids []Data `json:"centroids"`
	Instances []Data `json:"instances"`
}

type WorkerIp struct {
	MasterIp string `json:"master_ip"`
	Ip string `json:"ip"`
}

func (w WorkerIp) toJson() string {
	json, _ := json.Marshal(w)
	return string(json)
} 

type MasterIp struct {
	ip string 
	listenerWorkerPort string
	listenerAppPort string
}

type InstanceDistance  struct{
	clusterId int 
	distance float64
}

type CombineResult struct {
	Total []float64 `json:"total"`
	Instances []Data`json:"instances"`
}

type Solution struct {
	Data map[int] CombineResult `json:"Data"`
} 

var response Solution

type WorkerRequest struct {
	Instances []Data `json:"instances"`
	Centroids []Data `json:"centroids"`
}

type CentroidPartial struct{
	partialSum []float64
	contInstance int
}

type SafeMapReduce struct {
	v map[int] []float64
	conts map[int] int  
	mux sync.Mutex
}
func (c *SafeMapReduce) Inc(s Solution){
	c.mux.Lock()
		for k,v := range s.Data {
			if len(c.v[k]) == 0 {
				c.v[k] = make([]float64,len(v.Total))
			}
			c.v[k] = sumSlices(c.v[k],v.Total)
			c.conts[k] += len(v.Instances)
		}
	c.mux.Unlock()
}


func sumSlices(s1[]float64, s2[]float64) (result[]float64){
	for i := range s1 {	
		result = append(result,s1[i]+s2[i])
	}
	return
}

var request WorkerRequest
var centroids [][]float64
var dataSet []Data
var contSolution int
var safeMapReduce SafeMapReduce = SafeMapReduce{
	v:make(map[int][]float64),
	conts: make(map[int]int)}

func registerWorker(){
	ginMasterIp := bufio.NewReader(os.Stdin)
	fmt.Print("Registra la ip:port de mi master: ")
	masterIp, _ := ginMasterIp.ReadString('\n')
	masterIp = strings.TrimSpace(masterIp)

	ginIp := bufio.NewReader(os.Stdin)
	fmt.Print("Registra mi ip:port : ")
	ip, _ := ginIp.ReadString('\n')
	ip = strings.TrimSpace(ip)

	workerInfo = WorkerIp{Ip: ip, MasterIp: masterIp}
	fmt.Printf("Mi info es %+v\n",workerInfo)
}

func registerMaster(){
	ginIp := bufio.NewReader(os.Stdin)
	fmt.Print("Registra mi ip: ")
	ip, _ := ginIp.ReadString('\n')
	ip = strings.TrimSpace(ip)

	ginWorkerPort := bufio.NewReader(os.Stdin)
	fmt.Print("Registra mi listener port para mis workers: ")
	workerPort, _ := ginWorkerPort.ReadString('\n')
	workerPort = strings.TrimSpace(workerPort)

	ginAppPort := bufio.NewReader(os.Stdin)
	fmt.Print("Registra mi listener port para la app: ")
	appPort, _ := ginAppPort.ReadString('\n')
	appPort = strings.TrimSpace(appPort)	

	masterInfo = MasterIp{ip:ip, listenerAppPort:appPort, listenerWorkerPort:workerPort}
	fmt.Printf("Mi info es %+v",masterInfo)
}

func handlerWorkers(){
	port := fmt.Sprintf("%s:%s",masterInfo.ip,masterInfo.listenerWorkerPort)
	ln, err := net.Listen("tcp",port)
	if err != nil {
		fmt.Println("Error when enable listener port for workers")
		os.Exit(1)
	}
	defer ln.Close()
	for {
		conn, errWorker := ln.Accept()
		if errWorker != nil {
			fmt.Println("Ha ocurrido un error en la recepcion de la solucion de un worker")
		}
		defer conn.Close()
		resp, _ := bufio.NewReader(conn).ReadString('\n')
		var respAux TCPResponse 
		json.Unmarshal([]byte(resp), &respAux)
		if respAux.Id == REGISTER_WORKER{
			var handleInfo WorkerIp
			json.Unmarshal([]byte(respAux.Data), &handleInfo)
			workerIps = append(workerIps,handleInfo.Ip)
			fmt.Fprintln(conn,"Has sido registrado")
		}
		if respAux.Id == SOLUTION_WORKER{
			var handleResp Solution
			json.Unmarshal([]byte(respAux.Data),&handleResp)
			Reduce(handleResp)
		}
		 
	}

}

func Reduce(resp Solution) {
	safeMapReduce.Inc(resp);
	contSolution += 1
	if (contSolution == 2){
		updateCentroids()
		contSolution = 0
	}
	
}

func updateCentroids() {
	 for k,v := range safeMapReduce.v {
		fmt.Printf("\nCentroid updated %v",sumTotalCentroids(v,safeMapReduce.conts[k]))
	} 

}

func sumTotalCentroids( dimensions[]float64,length int)(result[]float64){
	for _,v := range dimensions {
		result = append(result,v / float64(length))
	}
	return
}


func notifyMeToMaster(){
	conn, _ := net.Dial("tcp",workerInfo.MasterIp)
	defer conn.Close()
	fmt.Fprintln(conn,TCPResponse{Data:workerInfo.toJson(),Id:REGISTER_WORKER}.toJson())
	msg, _ := bufio.NewReader(conn).ReadString('\n')
	fmt.Println(msg) 
}

func sendSolution(){
	conn, _ := net.Dial("tcp",workerInfo.MasterIp)
	defer conn.Close()
	resp, _ := json.Marshal(response)
	fmt.Fprintln(conn,TCPResponse{Data:string(resp),Id:SOLUTION_WORKER}.toJson())
	msg, _ := bufio.NewReader(conn).ReadString('\n')
	fmt.Println(msg) 
}

func handlerMaster(){
	ln, err := net.Listen("tcp",workerInfo.Ip)
	defer ln.Close()
	if err != nil {
		fmt.Println("Error when enable listener port for master")
		os.Exit(1)
	}
	for {
		conn, errWorker := ln.Accept()
		if errWorker != nil {
			fmt.Println("Ha ocurrido un error en la recepcion de la peticion del master")
		}
		req, _ := bufio.NewReader(conn).ReadString('\n')
		json.Unmarshal([]byte(req),&request)
		groupInstancesByCentroid()
	}
}

func init(){
	dataSet = []Data{
		{
			Id: 1,
			Dimensions: []float64{30.0,50.0},
			Cluster:1,	
		},
		{
			Id: 2,
			Dimensions: []float64{80.0,100.0},
			Cluster:2,	
		},
		{
			Id: 3,
			Dimensions: []float64{140.0,300.0},	
			Cluster:3,
		},
		{
			Id: 4,
			Dimensions: []float64{500.0,100.0},	
			Cluster:4,
		},
		{
			Id: 5,
			Dimensions: []float64{700.0,400.0},	
			Cluster:5,
		},
		{
			Id: 6,
			Dimensions: []float64{90.0,200.0},	
			Cluster:6,
		},
		{
			Id: 7,
			Dimensions: []float64{91.0,202.0},	
			Cluster:7,
		},
		{
			Id: 8,
			Dimensions: []float64{93.0,203.0},	
			Cluster:8,
		},
		{
			Id: 9,
			Dimensions: []float64{94.0,204.0},	
			Cluster:9,
		},
	}

}


func master(){
	fmt.Print("Soy el master")
}

func CalculateDistance(instance Data, centroid Data) float64{
	var distance float64 
	for i := range instance.Dimensions {
		distance += math.Pow(instance.Dimensions[i] - centroid.Dimensions[i], 2.0)
	}
	return math.Sqrt(distance)
}

func FindClosestCluster(InstanceDistances []InstanceDistance) (min InstanceDistance){
	if InstanceDistances == nil {
		return 
	}
	min = InstanceDistances[0]
	for _,v := range InstanceDistances {
		if v.distance < min.distance {
			min = v
		}
	}
	return 
}

func sumDimensions(total []float64, newInstance Data )(clusterDimension[]float64){
	if total == nil {
		clusterDimension = newInstance.Dimensions
		return
	}
	for i,v := range newInstance.Dimensions {
		clusterDimension = append(clusterDimension,total[i] + v)
	}
	return
}


func groupInstancesByCentroid() {
	response.Data = make(map[int]CombineResult)
	for _,instance := range request.Instances{
		var distances []InstanceDistance
		for _,centroid := range request.Centroids{
			distances = append(distances,InstanceDistance{clusterId: centroid.Id, distance: CalculateDistance(instance,centroid)})
		}
		closestCluster := FindClosestCluster(distances)
		currentCluster :=  response.Data[closestCluster.clusterId]
		response.Data[closestCluster.clusterId] = CombineResult{
			Instances: append(currentCluster.Instances,instance),
			Total : sumDimensions(currentCluster.Total,instance),
		}
	}
	sendSolution()
}

func worker(){
	fmt.Print("Soy un worker")
}

func sendChunk(chunk []Data, centroids []Data,workerIp string){
	conn, err	:= net.Dial("tcp",workerIp)
	 if err != nil {
		fmt.Println("Error reading:", err.Error())
		os.Exit(1)
	 }
	defer conn.Close()
	req, _ := json.Marshal(WorkerRequest{Centroids: centroids, Instances: chunk})
	fmt.Fprintln(conn,string(req))
}

func chunksInitialize(){
	for {
		if len(workerIps) > 1 {
			centroids := []Data{
				dataSet[1],
				dataSet[2],
			}
			chunkLen := len(dataSet) / len(workerIps)
			for i := range workerIps {
				var chunks []Data
				chunks = dataSet[chunkLen*i:chunkLen*(i+1)]
				if  i == len(workerIps) - 1 && len(dataSet) % len(workerIps) > 0 {
					chunks = append(chunks,dataSet[chunkLen*(i+1):]...)
				}
				sendChunk(chunks,centroids,workerIps[i])	
			}
			break;
		}
	}


}

func main(){
	ginRol:= bufio.NewReader(os.Stdin)
	fmt.Print("Registra mi Rol (Master: 0, Worker: 1): ")
	resp,_ := ginRol.ReadString('\n')
	resp = strings.TrimSpace(resp)
	respInt,_ := strconv.Atoi(resp)
	switch respInt {
	case 0:
		rol = MASTER
		registerMaster()
		go handlerWorkers()
		go chunksInitialize()
		break
	case 1: 
		rol = WORKER
		registerWorker()
		notifyMeToMaster()
		go handlerMaster()
		break
	default:
		err := fmt.Errorf("Error rol")
		fmt.Println(err)
		os.Exit(1)
		break;
	}

	for {

	}
}