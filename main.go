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
	"strings"
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
	// wg           sync.WaitGroup
	clientID     string
	clientSecret string
	userName     string
	password     string
	hostName     string
	foundLinks   uint32
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
	startTime := time.Now().UnixNano()

	initEnv()

	flag.Parse()
	args := flag.Args()
	if len(args) < 1 {
		log.Fatal("Please enter a csv")
	}

	// open file
	f, err := os.Open(args[0])
	checkerr(err)
	defer f.Close()

	ds := createData(f)

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

	fmt.Printf("accessTokTime: %#v\n\n", accessTokTime)

	for _, s := range ds[1:] {
		// 	// wg.Add(1)
		checkURL(s, rd.AccessToken)

		if ((time.Now().UnixNano() - accessTokTime) / 1000000000.0) > 3500 {
			resp, err := http.PostForm(hostName+"/oauth/v2/token", url.Values{"grant_type": {"refresh_token"}, "client_id": {clientID}, "client_secret": {clientSecret}, "refresh_token": {rd.RefreshToken}})
			if err != nil {
				log.Fatalf("Error: %#v\n", err)
			}
			defer resp.Body.Close()

			byteData, err := ioutil.ReadAll(resp.Body)
			json.Unmarshal(byteData, &rd)

			// fmt.Printf("\nNew AccessToken: %#v\n", rd.AccessToken)
			// fmt.Printf("ExpiresIn: %#v\n", rd.ExpiresIn)
			// fmt.Printf("RefreshToken: %#v\n", rd.RefreshToken)
			// fmt.Printf("Scope: %#v\n\n", rd.Scope)

		}
	}

	// Wait for all HTTP fetches to complete.
	// wg.Wait()
	fmt.Printf("Links OK: %d\n", foundLinks)
	fmt.Printf("\nDuration: %#.4v min.\n", float64((time.Now().UnixNano()-startTime)/1000000000.0)/60.0)
}

func checkURL(u diigoCsv, token string) {
	// defer wg.Done()

	// c := &http.Client{
	// 	Timeout: 10 * time.Second,
	// }

	resp, err := http.Get(u.url)
	if err != nil {
		// log.Printf("Warning: %#v, %#v\n", u.url, err)
	} else if resp.StatusCode == 200 {
		foundLinks++
		// fmt.Printf("Info: %#v\n", u)
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

		c := &http.Client{}

		resp, err = c.Do(req)
		if err != nil {
			log.Fatalf("Post Error: %#v - %#v\n", u, err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			log.Printf("Info: %#v - %#v\n", resp.StatusCode, u.url)
		}
	} else {
		// log.Printf("Warning: %#v - %#v\n", resp.StatusCode, u.url)
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
