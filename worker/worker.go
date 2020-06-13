package main
import (
	/* 
	"strconv"
	*/
	"os"
	"net"
	"math"
	"fmt"
	"bufio"
	"strings"
	"encoding/json"
	/* "sync" */
)
const (
	REGISTER_WORKER = 2
	SOLUTION_WORKER = 3
)

type WorkerRequest struct {
	Instances []Data `json:"instances"`
	Centroids []Data `json:"centroids"`
}

type WorkerInfo struct {
	MasterIp string `json:"master_ip"`
	Ip string `json:"ip"`
}
func (w WorkerInfo) toJson() string {
	json, _ := json.Marshal(w)
	return string(json)
}

type Data struct {
	Id int `json:"id"`
	Dimensions  []float64 `json:"Dimensions"`
}
func (data Data) toJson() string {
	json, _ := json.Marshal(data)
	return string(json)
}

type TCPMessage struct{
	Id int `json:"id"`
	Data string `json:"data"`
}

func (t TCPMessage) toJson() string {
	json, _ := json.Marshal(t)
	return string(json)
}
type CombineResult struct {
	Total []float64 `json:"total"`
	Instances []Data`json:"instances"`
} 

type Solution struct {
	Data map[int] CombineResult `json:"Data"`
}

type InstanceDistance  struct{
	clusterId int 
	distance float64
}



func notifyMeToMaster(){
	conn, _ := net.Dial("tcp",workerInfo.MasterIp)
	defer conn.Close()
	fmt.Fprintln(conn,TCPMessage{Data:workerInfo.toJson(),Id:REGISTER_WORKER}.toJson())
	msg, _ := bufio.NewReader(conn).ReadString('\n')
	fmt.Println(msg) 
}

func sendSolution(){
	conn, _ := net.Dial("tcp",workerInfo.MasterIp)
	defer conn.Close()
	resp, _ := json.Marshal(response)
	fmt.Fprintln(conn,TCPMessage{Data:string(resp),Id:SOLUTION_WORKER}.toJson())
	msg, _ := bufio.NewReader(conn).ReadString('\n')
	fmt.Println(msg) 
}

func registerWorker(){
	ginMasterIp := bufio.NewReader(os.Stdin)
	fmt.Print("Registra la ip:port de mi master: ")
	masterIp, _ := ginMasterIp.ReadString('\n')
	masterIp = strings.TrimSpace(masterIp)

	ginIp := bufio.NewReader(os.Stdin)
	fmt.Print("Registra mi ip:port : ")
	ip, _ := ginIp.ReadString('\n')
	ip = strings.TrimSpace(ip)

	workerInfo = WorkerInfo{Ip: ip, MasterIp: masterIp}
	fmt.Printf("Mi info es %+v\n",workerInfo)
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


var workerInfo WorkerInfo
var request WorkerRequest
var response Solution

func init(){
	registerWorker()
}


func main(){
	notifyMeToMaster()
	go handlerMaster()
	for {
		
	}
}