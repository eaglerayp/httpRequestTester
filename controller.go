package main

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"runtime"
	"sync"

	"time"

	"github.com/valyala/fasthttp"
)

var httpClient = &fasthttp.Client{
	Name:      "Push Controller",
	TLSConfig: &tls.Config{InsecureSkipVerify: true},
}

type Task struct {
	slot   int
	offset int
}

var taskQ = make(chan Task, 10000)
var timeout = 30 * time.Second
var wg sync.WaitGroup

const (
	slotURI = "https://10.128.81.202:19975/droicc/lua/cloudcodes/skip.lua"
	sortURI = "https://10.128.81.202:19975/droicc/lua/cloudcodes/sort.lua"
)

// This is a http client controller for cloudcode trigger
func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	start := time.Now()
	// singleThreadSort()
	slot()
	elapsed := time.Since(start)
	fmt.Println("Used:", elapsed)
}

func slot() {

	// uri := "http://127.0.0.1:8081/droicc/lua/cloudcodes/slot.lua"

	thread := 10
	totalSlot := 1
	offsetMax := 3000000 / totalSlot
	fmt.Println("offsetMax:", offsetMax)
	for s := 0; s < totalSlot; s++ {
		go func(slot int) {
			for offset := 0; offset < offsetMax; offset += 1000 {
				taskQ <- Task{slot: slot, offset: offset}
				wg.Add(1)
				// fmt.Println("offset:", offset)
			}
		}(s)
	}
	time.Sleep(1 * time.Second)
	for i := 0; i < thread; i++ {
		go httpTaskConsumer()
	}
	wg.Wait()
	// multithread
}

func httpTaskConsumer() {
	for task := range taskQ {
		req := fasthttp.AcquireRequest()
		req.SetRequestURI(slotURI)
		resp := fasthttp.AcquireResponse()
		defer fasthttp.ReleaseRequest(req)
		defer fasthttp.ReleaseResponse(resp)
		req.Header.SetMethod("POST")
		req.Header.Add("Content-Type", "application/json")
		req.Header.Add("X-Droi-AppID", "s37umbzhjqGzAspf-YGLNcextiCxJbLPlQCXGv0l")
		// req.Header.Add("X-Droi-AppID", "s37umbzhFw9R5r33eTy9dvxIp1KdgaY9lQDzZK0M")
		// req.Header.Add("X-Droi-Service-AppID", "s37umbzhjqGzAspf-YGLNcextiCxJbLPlQCXGv0l")
		jsonDoc := map[string]interface{}{}
		jsonDoc["Slot"] = task.slot
		jsonDoc["Skip"] = task.offset

		body, _ := json.Marshal(jsonDoc)
		req.SetBody(body)
		// fmt.Println(req)
		httpErr := httpClient.DoTimeout(req, resp, timeout)
		if httpErr != nil {
			log.Println("HTTP Error:", httpErr)
			taskQ <- task
			continue
		}
		// parse response get id
		if resp.StatusCode() != fasthttp.StatusOK {
			log.Println("StatusCode:", resp.StatusCode())
			log.Println("header:", resp.Header.String())
			taskQ <- task
			continue
		}
		result := map[string]interface{}{}
		err := json.Unmarshal(resp.Body(), &result)
		if err != nil {
			log.Println("Json unmarshal:", err)
			log.Println("result:", string(resp.Body()))
			taskQ <- task
			continue
		}
		// fmt.Println("result:", result)
		if result["Code"].(float64) != 0 {
			log.Println("Code !=0:", result)
			taskQ <- task
			continue
		}
		count := result["Result"].(map[string]interface{})["count"].(float64)
		log.Println("task slot:", task.slot, " offset:", task.offset, "count:", count)
		wg.Done()
	}
}

func singleThreadSort() {
	id := ""

	timeout := 30 * time.Second

	for count := 0; count < 3000; count++ {
		req := fasthttp.AcquireRequest()
		req.SetRequestURI(sortURI)
		resp := fasthttp.AcquireResponse()
		defer fasthttp.ReleaseRequest(req)
		defer fasthttp.ReleaseResponse(resp)
		req.Header.SetMethod("POST")
		req.Header.Add("Content-Type", "application/json")
		req.Header.Add("X-Droi-AppID", "s37umbzhFw9R5r33eTy9dvxIp1KdgaY9lQDzZK0M")
		// req.Header.Add("X-Droi-Service-AppID", "s37umbzhjqGzAspf-YGLNcextiCxJbLPlQCXGv0l")
		jsonDoc := make(map[string]string)
		if len(id) > 0 {
			jsonDoc["Id"] = id
		}
		body, _ := json.Marshal(jsonDoc)
		req.SetBody(body)
		// fmt.Println(req)
		httpErr := httpClient.DoTimeout(req, resp, timeout)
		if httpErr != nil {
			log.Println("HTTP Error:", httpErr)
			continue
		}
		// parse response get id
		if resp.StatusCode() != fasthttp.StatusOK {
			log.Println("StatusCode:", resp.StatusCode())
			log.Println("header:", resp.Header.String())
			continue
		}
		result := map[string]interface{}{}
		err := json.Unmarshal(resp.Body(), &result)
		if err != nil {
			log.Println("Json unmarshal:", err)
		}
		// fmt.Println("result:", result)
		if result["Code"].(float64) == 0 {
			id = result["Result"].(map[string]interface{})["Id"].(string)
			fmt.Println("Id:", id)
		}
	}
}
