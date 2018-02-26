package main

import (
	"encoding/csv"
	"encoding/json"
	"errors"
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
	"sync/atomic"
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
	workCounter  int32
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

	concurrencyPtr := flag.Int("t", 1, "how many threads startet")
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

	ds := createData(f)
	fmt.Printf("Entries: %d\n", len(ds))

	clientID = viper.GetString("settings.clientID")
	clientSecret = viper.GetString("settings.clientSecret")
	userName = viper.GetString("settings.username")
	password = viper.GetString("settings.password")
	hostName = viper.GetString("settings.host")

	resp, err := http.PostForm(hostName+"/oauth/v2/token",
		url.Values{"grant_type": {"password"}, "client_id": {clientID}, "client_secret": {clientSecret}, "username": {userName}, "password": {password}})

	if err != nil {
		log.Fatalf("Error: %#v\n", err)
	}

	defer resp.Body.Close()

	accessTokTime := time.Now().UnixNano()

	var rd respData
	byteData, err := ioutil.ReadAll(resp.Body)
	json.Unmarshal(byteData, &rd)

	fmt.Println("========================================================")

	sem := make(chan bool, *concurrencyPtr)

	for _, s := range ds[1:] {
		sem <- true

		go checkURL(sem, s, rd.AccessToken, finvalid, finsert)

		if ((time.Now().UnixNano() - accessTokTime) / 1000000000.0) > 3500 {
			resp, err := http.PostForm(hostName+"/oauth/v2/token", url.Values{"grant_type": {"refresh_token"}, "client_id": {clientID}, "client_secret": {clientSecret}, "refresh_token": {rd.RefreshToken}})
			if err != nil {
				log.Fatalf("Error: %#v\n", err)
			}
			defer resp.Body.Close()

			byteData, err := ioutil.ReadAll(resp.Body)
			json.Unmarshal(byteData, &rd)
		}

	}
	for i := 0; i < cap(sem); i++ {
		sem <- true
	}

	fmt.Printf("Links OK: %d\n", workCounter)
	fmt.Printf("\nDuration: %#.4v min.\n", float64((time.Now().UnixNano()-startTime)/1000000000.0)/60)
}

func retry(c http.Client, dc *diigoCsv) (resp *http.Response, err error) {
	for i := 0; i < 3; i++ {
		resp, err = c.Get(dc.url)
		if err == nil && resp.StatusCode == 200 {
			return resp, err
		}
		time.Sleep(1 * time.Microsecond)
	}

	ur, err := url.Parse(dc.url)
	if err != nil {
		log.Fatalf("Parse Error: %s\n", err)
	}

	for i := 0; i < 3; i++ {
		resp, err = c.Get(ur.Scheme + "://" + ur.Hostname())
		if err == nil && resp.StatusCode == 200 {
			dc.url = ur.Scheme + "://" + ur.Hostname()
			dc.title = ur.Hostname()
			dc.tags = ""
			dc.description = ""
			dc.comments = ""
			dc.annotations = ""
			return resp, err
		}
		time.Sleep(1 * time.Microsecond)
	}

	return nil, errors.New("Could not connect to: " + dc.url + ", " + ur.Scheme + "://" + ur.Hostname())
}

func checkURL(sem chan bool, u diigoCsv, token string, finvalid *os.File, finsert *os.File) {
	defer func() { <-sem }()

	getClient := http.Client{
		Timeout: time.Duration(3 * time.Minute),
	}

	resp, err := getClient.Get(u.url)
	if err != nil || resp.StatusCode != 200 {
		resp, err = retry(getClient, &u)
	}

	if err != nil || resp.StatusCode != 200 {
		finvalid.WriteString(fmt.Sprintf("Error: %s\n", err))
		return
	}

	atomic.AddInt32(&workCounter, 1)
	fmt.Printf("LINK: %d\n", workCounter)

	form := url.Values{}
	form.Add("url", u.url)
	form.Add("title", u.title)
	if u.tags != "no_tags" {
		form.Add("tags", u.tags)
	}
	form.Add("content", u.description)
	form.Add("published_at", u.createdAt)

	req, err := http.NewRequest("POST", hostName+"/api/entries.json", strings.NewReader(form.Encode()))
	if err != nil {
		log.Fatalf("Error: %#v\n", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Authorization", "Bearer "+token)

	cPost := http.Client{
		Timeout: time.Duration(10 * time.Minute),
	}

	resp, err = cPost.Do(req)
	if err != nil {
		finsert.WriteString(fmt.Sprintf("Post Error: %s - %s\n", u, err))
		// log.Fatalf("Post Error: %s - %s\n", u, err)
	}

	if resp != nil {
		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			finsert.WriteString(fmt.Sprintf("Warning: %d - %s\n", resp.StatusCode, u.url))
		}
	} else {
		finsert.WriteString(fmt.Sprintf("Warning: No Response - %s\n", u.url))
	}
}

func createData(f *os.File) []diigoCsv {
	r := csv.NewReader(f)

	ds := make([]diigoCsv, 0, 1000)

	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		checkerr(err)

		s := diigoCsv{}
		s.title = record[0]
		s.url = record[1]
		s.tags = record[2]
		s.description = record[3]
		s.comments = record[4]
		s.annotations = record[5]
		s.createdAt = record[6]

		ds = append(ds, s)
	}

	return ds
}

func checkerr(err error) {
	if err != nil {
		panic(err)
	}
}
