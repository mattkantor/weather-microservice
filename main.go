package main

import (
	"fmt"
	"log"
	"encoding/json"
	"io/ioutil"
	"time"
	"flag"
	"net/http"
	redis "github.com/go-redis/redis/v7" 
)

type App struct{
	httpPort string
	redisClient *redis.Client
	ws *WeatherService
}


type WeatherService struct{
	apiKey string
}


func NewWeatherService(apiKey string) *WeatherService{
	return &WeatherService{ apiKey:apiKey }
}

func main(){
	var wsApiKey = flag.String("weatherApiKey", "", "the api key for openweatherAPI")
	var port = flag.String("httpPort","8090", "port to run this microservice");
	flag.Parse()
	if port==nil || wsApiKey == nil{
		return
	}
	client := NewRedisClient()
	app := App{
		httpPort: *port,
		redisClient:client,
		ws: NewWeatherService(*wsApiKey),
	}
	app.start()
}


func (app *App) start(){
	
	http.HandleFunc("/weather", app.getWeather)
    http.ListenAndServe(":"+app.httpPort, nil)
}

func (app *App)getWeather(w http.ResponseWriter, req *http.Request) {
	
	w.Header().Set("Content-Type", "application/json")
	
	keys, ok := req.URL.Query()["city"]
    
    if !ok || len(keys[0]) < 1 {
        log.Println("Url Param 'city' is missing")
        return
	}
	city := keys[0]
	weather := JsonWeatherResponse{}
	weatherJsonString, err  := app.redisClient.Get(city).Result();
	if err == redis.Nil || err != nil {
		//ok no record found
		weather =app.ws.getWeatherFromService(city);
	
		weatherString, _ := json.Marshal(weather);
		redisErr := app.redisClient.Set(city, string(weatherString), time.Minute * 600).Err()
		if redisErr!=nil {
			panic(err);
		}
	}else{
		_ = json.Unmarshal([]byte(weatherJsonString), &weather)

	}

	json.NewEncoder(w).Encode(weather)
	return 
	
}


func (ws *WeatherService)getWeatherFromService(city string) JsonWeatherResponse{
	
	baseUrl := "https://api.openweathermap.org/data/2.5/forecast?q=%s&appid=%s"

	weatherClient := http.Client{
		Timeout: time.Second * 2, // Maximum of 2 secs
	}

	url := fmt.Sprintf(baseUrl, city, ws.apiKey);
	fmt.Println(url);
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		log.Fatal(err)
	}
	res, getErr := weatherClient.Do(req)
	if getErr != nil {
		log.Fatal(getErr)
	}

	body, readErr := ioutil.ReadAll(res.Body)
	if readErr != nil {
		log.Fatal(readErr)
	}
	weatherData := JsonWeatherResponse{}
	jsonErr := json.Unmarshal(body, &weatherData)
	if jsonErr != nil {
		log.Fatal(jsonErr)
	}
	return weatherData;


}

func NewRedisClient() *redis.Client {
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	_, err := client.Ping().Result()
	if err != nil {
		panic(err)
	}
	return client
}

type JsonWeatherResponse struct {
	Cod     string `json:"cod"`
	Message int    `json:"message"`
	Cnt     int    `json:"cnt"`
	List    []struct {
		Dt   int `json:"dt"`
		Main struct {
			Temp      float64 `json:"temp"`
			FeelsLike float64 `json:"feels_like"`
			TempMin   float64 `json:"temp_min"`
			TempMax   float64 `json:"temp_max"`
			Pressure  int     `json:"pressure"`
			SeaLevel  int     `json:"sea_level"`
			GrndLevel int     `json:"grnd_level"`
			Humidity  int     `json:"humidity"`
			TempKf    float64 `json:"temp_kf"`
		} `json:"main"`
		Weather []struct {
			ID          int    `json:"id"`
			Main        string `json:"main"`
			Description string `json:"description"`
			Icon        string `json:"icon"`
		} `json:"weather"`
		Clouds struct {
			All int `json:"all"`
		} `json:"clouds"`
		Wind struct {
			Speed float64 `json:"speed"`
			Deg   int     `json:"deg"`
		} `json:"wind"`
		Snow struct {
			ThreeH float64 `json:"3h"`
		} `json:"snow,omitempty"`
		Sys struct {
			Pod string `json:"pod"`
		} `json:"sys"`
		DtTxt string `json:"dt_txt"`
		Rain  struct {
			ThreeH float64 `json:"3h"`
		} `json:"rain,omitempty"`
	} `json:"list"`
	City struct {
		ID    int    `json:"id"`
		Name  string `json:"name"`
		Coord struct {
			Lat float64 `json:"lat"`
			Lon float64 `json:"lon"`
		} `json:"coord"`
		Country    string `json:"country"`
		Population int    `json:"population"`
		Timezone   int    `json:"timezone"`
		Sunrise    int    `json:"sunrise"`
		Sunset     int    `json:"sunset"`
	} `json:"city"`
}