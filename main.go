package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"runtime/pprof"
	"runtime/trace"
	"strings"
	"sync"
	"time"

	"github.com/spf13/viper"
)

type diigoCsv struct {
	title       string
	url         string
	tags        string
	description string
	comments    string
	annotations string
	createdAt   string
	checkResp    int
	insertResp    int
	workN    int
}

type respData struct {
	AccessToken  string `json:"access_token"`
	ExpiresIn    int    `json:"expires_in"`
	TokenType    string `json:"token_type"`
	Scope        string
	RefreshToken string `json:"refresh_token"`
}

var (
	clientID     string
	clientSecret string
	userName     string
	password     string
	hostName     string
)

func initEnv() {
	viper.SetConfigName("config")
	viper.SetConfigType("toml")
	viper.AddConfigPath(".")

	err := viper.ReadInConfig() // Find and read the config file
	if err != nil {             // Handle errors reading the config file
		log.Fatalf("Fatal error config file: %s \n", err)
	}
}

func main() {
	c, err := os.Create("cpu.pprof")
	checkerr(err)
	t, err := os.Create("trace.log")
	checkerr(err)

	pprof.StartCPUProfile(c)
	defer pprof.StopCPUProfile()

	trace.Start(t)
	defer trace.Stop()

	startTime := time.Now().UnixNano()

	initEnv()

	concurrencyPtr := flag.Int("t", 1, "how many goroutines startet")
	debugPtr := flag.Bool("d", false, "write debug files")

	flag.Parse()
	args := flag.Args()
	if len(args) < 1 {
		log.Fatal("Please enter a csv")
	}

	// open file
	f, err := os.Open(args[0])
	checkerr(err)
	defer f.Close()

	var finvalid, finsert *os.File

	if *debugPtr {
		finvalid, err = os.Create("invalid_link.log")
		checkerr(err)

		finsert, err = os.Create("insert_link.log")
		checkerr(err)
	}

	in := genData(f)
	
	clientID = viper.GetString("settings.clientID")
	clientSecret = viper.GetString("settings.clientSecret")
	userName = viper.GetString("settings.username")
	password = viper.GetString("settings.password")
	hostName = viper.GetString("settings.host")

	fmt.Println("======================== Start Pipeline ========================")

	done := make(chan bool)
	tokch := make(chan string)
	go tokGen(tokch, done)

	out := make(chan diigoCsv)

	var wg sync.WaitGroup
	wg.Add(*concurrencyPtr)

	for i := 0; i < *concurrencyPtr; i++ {
		go func(i int) {
			checkURLWorker(i, finvalid, in, out) // HLc
			wg.Done()
		}(i)
	}
	go func() {
		wg.Wait()
		close(out) // HLc
	}()

	result := make(chan diigoCsv) // result channel

	var wg2 sync.WaitGroup
	wg2.Add(*concurrencyPtr)

	for i := 0; i < *concurrencyPtr; i++ {
		go func(i int) {
			insertURLWorker(i, finsert, tokch, out, result) // HLc
			wg2.Done()
		}(i)
	}
	go func() {
		wg2.Wait()
		close(result) // HLc
	}()

	// End of pipeline. OMIT

	var okC int
	var allC int
	for r := range result {
		allC++

		if r.insertResp == 200 {
			okC++
		}		
	}

	done <- true

	fmt.Println("======================== END Pipeline   ========================")

	fmt.Printf("Links: %d\n", allC)
	fmt.Printf("Links OK: %d\n", okC)
	fmt.Printf("\nDuration: %#.4v min.\n", float64((time.Now().UnixNano()-startTime)/1000000000.0)/60)
}

func checkURLWorker(i int, finvalid *os.File, in <-chan diigoCsv, out chan<- diigoCsv) {
	getClient := http.Client{
		Timeout: time.Duration(5 * time.Second),
	}

	for diigo := range in {
		resp, err := getClient.Get(diigo.url)
		if err != nil || resp.StatusCode != 200 {
			retry(getClient, &diigo, finvalid)
		} else {
			diigo.checkResp = resp.StatusCode
		}
		// fmt.Printf("CheckUrlWorker: %d, URL: %s, HttpCode: %d, Title: %s\n", i, diigo.url, diigo.httpResp, diigo.title)
		out <- diigo
	}
}

func insertURLWorker(i int, f *os.File, tch <-chan string, in <-chan diigoCsv, res chan<- diigoCsv) {
	cPost := http.Client{
		Timeout: time.Duration(1 * time.Minute),
	}

	for diigo := range in {
		if diigo.checkResp == 200 {
			form := url.Values{}
			form.Add("url", diigo.url)
			form.Add("title", diigo.title)
			if diigo.tags != "no_tags" {
				form.Add("tags", diigo.tags)
			}
			form.Add("content", diigo.description)
			form.Add("published_at", diigo.createdAt)

			req, err := http.NewRequest("POST", hostName+"/api/entries.json", strings.NewReader(form.Encode()))
			if err != nil {
				log.Fatalf("Error: %#v\n", err)
			}
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			
			token := <-tch
			// fmt.Printf("InsertWorker: %d, Title: %s, Token:: %s\n", i, diigo.title, token)
			req.Header.Set("Authorization", "Bearer "+token)

			resp, err := cPost.Do(req)
			if err != nil {
				// log.Fatalf("Post Error: %s - %s\n", diigo.url, err)
				f.WriteString(fmt.Sprintf("Post Error: %s - %s\n", diigo.url, err))
			}

			if resp != nil {
				defer resp.Body.Close()

				if resp.StatusCode != 200 {
					f.WriteString(fmt.Sprintf("Warning: %d - %s\n", resp.StatusCode, diigo.url))
				} else {
					diigo.insertResp = 200
				}
			} else {
				f.WriteString(fmt.Sprintf("Warning: No Response - %s\n", diigo.url))
			}
		}

		diigo.workN = i
		res <- diigo
	}
}

func getToken() string {
	var rd respData
	resp, err := http.PostForm(hostName+"/oauth/v2/token",
		url.Values{"grant_type": {"password"}, "client_id": {clientID}, "client_secret": {clientSecret}, "username": {userName}, "password": {password}})
	
	if err != nil {
		log.Fatalf("Error: %#v\n", err)
	}
	defer resp.Body.Close()

	byteData, err := ioutil.ReadAll(resp.Body)
	json.Unmarshal(byteData, &rd)

	return rd.AccessToken
}

func tokGen(ch chan<- string, done <-chan bool) {
	ticker := time.NewTicker(time.Minute * 5)
	defer ticker.Stop()

	tok := getToken()

	for {
		select {
		case <-done:
			fmt.Println("Done!")
			return
		case ch <- tok:
		case <-ticker.C:
			tok = getToken()
			fmt.Println("New TickerToken: ", tok)
		}
	}
}

func retry(c http.Client, dc *diigoCsv, f *os.File) {
	for i := 0; i < 3; i++ {
		resp, err := c.Get(dc.url)
		if err == nil && resp.StatusCode == 200 {
			dc.checkResp = resp.StatusCode
		}
		time.Sleep(1 * time.Microsecond)
	}

	ur, err := url.Parse(dc.url)
	if err != nil {
		dc.checkResp = 400
		f.WriteString(fmt.Sprintf("Error: %#v\n", err))
		return
	}

	for i := 0; i < 3; i++ {
		resp, err := c.Get(ur.Scheme + "://" + ur.Hostname())
		if err == nil && resp.StatusCode == 200 {
			dc.url = ur.Scheme + "://" + ur.Hostname()
			dc.title = ur.Hostname()
			dc.tags = ""
			dc.description = ""
			dc.comments = ""
			dc.annotations = ""
			dc.checkResp = 200
			return
		}
		time.Sleep(1 * time.Microsecond)
	}
	dc.checkResp = 400
	f.WriteString(fmt.Sprintf("Error: %#v\n", dc.url))
}

func genData(f *os.File) <-chan diigoCsv {
	r := csv.NewReader(f)
	out := make(chan diigoCsv)

	go func() {
		for {
			record, err := r.Read()

			switch {
			case err == io.EOF:
				break
			case record[1] == "url":
				continue
			default:
				s := diigoCsv{}
				s.title = record[0]
				s.url = record[1]
				s.tags = record[2]
				s.description = record[3]
				s.comments = record[4]
				s.annotations = record[5]
				s.createdAt = record[6]

				out <- s
			}
		}
		close(out)
	}()

	return out
}

func checkerr(err error) {
	if err != nil {
		panic(err)
	}
}
