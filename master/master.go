package main 

import (
	"strconv"
	"io/ioutil"
	"errors"
	"math/rand"
	"strings"
	"fmt"
	"bufio"
	"os"
	"math"
	"net"
	"encoding/json"
	"sync"
	"net/http"
)

const (
	REGISTER_WORKER = 2
	SOLUTION_WORKER = 3
	MIN_WORKERS = 2 
)

type HttpRequest struct {
	KMeans string  `json:"k-means"`
	Threshold string `json:"threshold"`
	Iterations string `json:"iterations"`
}

type Data struct {
	Id int `json:"id"`
	Dimensions  []float64 `json:"Dimensions"`
}
func (data Data) toJson() string {
	json, _ := json.Marshal(data)
	return string(json)
}

type TCPResponse struct{
	Id int `json:"id"`
	Data string `json:"data"`
}

func (t TCPResponse) toJson() string {
	json, _ := json.Marshal(t)
	return string(json)
} 


var masterInfo MasterIp
var kMeans int 
var limits float64 
var iterations int 
var workerIps []string

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
			response[k] = append(response[k],v.Instances...)
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
var centroids []Data
var dataSet []Data
var response map[int][]Data = make(map[int][]Data)
var contSolution int
var contIteration int 
var limitReached bool
var safeMapReduce SafeMapReduce = SafeMapReduce{
	v:make(map[int][]float64),
	conts: make(map[int]int)}

var httpResp = make(chan bool) 

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
		go func(){
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
		}()
		 
	}

}

func Reduce(resp Solution) {
	safeMapReduce.Inc(resp);
	contSolution += 1
	if (contSolution == 2){
		updateCentroids()
		contSolution = 0
		contIteration += 1
		if  contIteration < iterations || !limitReached{
			safeMapReduce = SafeMapReduce{
				v:make(map[int][]float64),
				conts: make(map[int]int)}
			for k,_ := range response {
				response[k] = make([]Data,0)
			}
			chunksInitialize()
			
		}else{
			
			file, _ := json.MarshalIndent(response, "", " ")
			_ = ioutil.WriteFile("../data/response.json", file, 0644)
			httpResp <- true
		}
	}
	
	
}

func updateCentroids() {
	var contAux int 
	 for k,v := range safeMapReduce.v {
		dimensions  := sumTotalCentroids(v,safeMapReduce.conts[k])
		distance := CalculateDistancePoints(dimensions,centroids[k].Dimensions)
		if distance <= limits{
			contAux += 1
		}
		centroids[k].Dimensions = dimensions  
	}
	if contAux == len(centroids){
		limitReached = true
	}
}

func sumTotalCentroids( dimensions[]float64,length int)(result[]float64){
	for _,v := range dimensions {
		result = append(result,v / float64(length))
	}
	return
}




func init(){
	jsonFile, err := os.Open("../data/dataSet.json")
	defer jsonFile.Close()
	if err != nil {
		fmt.Println(err)
	}
	byteValue, _ := ioutil.ReadAll(jsonFile)
	json.Unmarshal(byteValue, &dataSet)
}



func CalculateDistancePoints(p1 []float64, p2 []float64) float64{
	var distance float64
	if len(p1) != len(p2) {
		err2 := errors.New("Las dimensiones de los puntos no coinciden")
		fmt.Println("Fatal Error:", err2.Error())
		os.Exit(1)
	} 
	for i := range p1 {
		distance += math.Pow(p1[i] - p2[i], 2.0)
	}
	return math.Sqrt(distance)
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
	chunkLen := len(dataSet) / len(workerIps)
			for i := range workerIps {
				var chunks []Data
				chunks = dataSet[chunkLen*i:chunkLen*(i+1)]
				if  i == len(workerIps) - 1 && len(dataSet) % len(workerIps) > 0 {
					chunks = append(chunks,dataSet[chunkLen*(i+1):]...)
				}
				sendChunk(chunks,centroids,workerIps[i])	
	}
}


func randomCentroids(){
	for i := 0; i < kMeans; i++ {
		newCentroid := Data{Id: i , Dimensions: dataSet[rand.Intn(len(dataSet))].Dimensions}
		centroids = append(centroids,newCentroid) 
	}
}
func HTTPHandlers(w http.ResponseWriter, r *http.Request) {
    if r.URL.Path != "/" {
        http.Error(w, "404 not found.", http.StatusNotFound)
        return
    }
 
    switch r.Method {
    case "GET":     
         http.ServeFile(w, r, "./index.html")
	case "POST":
		
		response = make(map[int][]Data)
		var req HttpRequest
		err := json.NewDecoder(r.Body).Decode(&req)
		if err != nil {
			fmt.Printf("%s",err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		iterationsAux, _ :=strconv.Atoi(req.Iterations)
		kMeansAux, _ := strconv.Atoi(req.KMeans)
		limitsAux, _ := strconv.ParseFloat(req.Threshold,64)

		iterations,limits,kMeans = iterationsAux, limitsAux, kMeansAux
		randomCentroids()
		chunksInitialize()
		

		w.Header().Set("Content-Type", "application/json")
		<-httpResp
		resp, err := json.Marshal(response)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		  } 
		w.Write(resp)
        
    default:
        fmt.Fprintf(w, "Sorry, only GET and POST methods are supported.")
    }
}

func main(){

	rand.Seed(42)
	registerMaster()
	randomCentroids()
	go handlerWorkers()

	http.HandleFunc("/", HTTPHandlers)
 
    fmt.Printf("\nStarting HTTP SERVER ON %s PORT...\n",masterInfo.listenerAppPort)
    if err := http.ListenAndServe(fmt.Sprintf(":%s",masterInfo.listenerAppPort), nil); err != nil {
        fmt.Printf("Error %s",err.Error())
    }
}